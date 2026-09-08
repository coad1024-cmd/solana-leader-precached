use {
    solana_clock::Epoch,
    solana_epoch_schedule::EpochSchedule,
    solana_leader_schedule::LeaderSchedule,
    solana_pubkey::Pubkey,
    solana_vote::vote_account::{VoteAccount, VoteAccountsHashMap},
    solana_leader_precached::{
        LeaderScheduleCache, LeaderSchedulePreCacher, PreCacherConfig, RpcError,
        SlotLeaderRpcService, StakeDataProvider,
    },
    std::{
        collections::HashMap,
        sync::Arc,
        time::Duration,
    },
};

/// Mock stake data provider creating synthetic vote accounts with weighted stakes.
pub struct MockStakeDataProvider {
    stakes_by_epoch: HashMap<Epoch, VoteAccountsHashMap>,
}

impl MockStakeDataProvider {
    pub fn new() -> Self {
        Self {
            stakes_by_epoch: HashMap::new(),
        }
    }

    pub fn with_epoch_stakes(
        mut self,
        epoch: Epoch,
        validators: &[(Pubkey, u64)],
    ) -> Self {
        let mut vote_accounts = VoteAccountsHashMap::default();
        for &(_node_pubkey, stake) in validators {
            let vote_account = VoteAccount::new_random();
            // Store with the specified node_pubkey by using node_pubkey as the key and updating VoteAccount
            let vote_pubkey = *vote_account.node_pubkey();
            vote_accounts.insert(vote_pubkey, (stake, vote_account));
        }
        self.stakes_by_epoch.insert(epoch, vote_accounts);
        self
    }

    pub fn with_shared_vote_accounts(
        mut self,
        epoch: Epoch,
        vote_accounts: VoteAccountsHashMap,
    ) -> Self {
        self.stakes_by_epoch.insert(epoch, vote_accounts);
        self
    }
}

impl StakeDataProvider for MockStakeDataProvider {
    fn get_epoch_vote_accounts(&self, epoch: Epoch) -> Option<VoteAccountsHashMap> {
        self.stakes_by_epoch.get(&epoch).cloned()
    }
}

fn create_test_validators() -> (Vec<(Pubkey, u64)>, Vec<Pubkey>) {
    let mut entries = Vec::new();
    let mut pubkeys = Vec::new();
    for i in 1..=5 {
        let pk = Pubkey::new_unique();
        entries.push((pk, i * 1_000_000_000));
        pubkeys.push(pk);
    }
    (entries, pubkeys)
}

#[test]
fn test_audit_gap_reproduction_without_precacher() {
    // 1. Setup an EpochSchedule with 200 slots per epoch (matching Agave Issue #6845 reproduction setup)
    let slots_per_epoch = 200;
    let epoch_schedule = EpochSchedule::custom(slots_per_epoch, slots_per_epoch, false);
    let (val_entries, _) = create_test_validators();

    let stake_provider = Arc::new(
        MockStakeDataProvider::new()
            .with_epoch_stakes(0, &val_entries)
            .with_epoch_stakes(1, &val_entries)
            .with_epoch_stakes(2, &val_entries)
    );

    // Initial cache contains Epoch 0 and 1
    let cache = Arc::new(LeaderScheduleCache::new(epoch_schedule, 1));
    let rpc_service = SlotLeaderRpcService::new(cache.clone());

    // Pre-populate Epoch 0 and 1 schedules
    let vote_accounts_0 = stake_provider.get_epoch_vote_accounts(0).unwrap();
    let schedule_0 = LeaderSchedule::new(
        &vote_accounts_0,
        0,
        slots_per_epoch as usize,
        solana_leader_schedule::NUM_CONSECUTIVE_LEADER_SLOTS,
    );
    cache.warm_leader_schedule(0, Arc::new(schedule_0));

    let vote_accounts_1 = stake_provider.get_epoch_vote_accounts(1).unwrap();
    let schedule_1 = LeaderSchedule::new(
        &vote_accounts_1,
        1,
        slots_per_epoch as usize,
        solana_leader_schedule::NUM_CONSECUTIVE_LEADER_SLOTS,
    );
    cache.warm_leader_schedule(1, Arc::new(schedule_1));

    // Verify Epoch 0 and 1 are cached, but Epoch 2 is missing
    assert!(cache.has_epoch(0));
    assert!(cache.has_epoch(1));
    assert!(!cache.has_epoch(2));

    // 2. Simulate Tower BFT 32-lockout root lag at the epoch boundary
    // Tip enters Epoch 1: slot 200..230 (the first 31 slots of Epoch 1)
    // Root lags by 31 slots: root_slot = tip - 31 = 230 - 31 = 199 (STILL in Epoch 0!)
    let tip_slot = 230;
    let root_slot = tip_slot - 31; // 199 (Epoch 0)
    cache.set_root_slot(root_slot);

    // Because root_slot (199) is in Epoch 0:
    // epoch_schedule.get_leader_schedule_epoch(199) is STILL Epoch 1.
    // Therefore, Epoch 2 is NEVER computed or rooted by the standard set_root mechanism.
    assert_eq!(cache.max_epoch(), 1);
    assert!(!cache.has_epoch(2));

    // 3. Client (TPU client or external RPC) performs lookahead across the epoch boundary
    // Asking for 100 slots starting at slot 350 (spans slots 350..450, requiring Epoch 2)
    let query_start = 350;
    let query_limit = 100;
    let result = rpc_service.get_slot_leaders(query_start, query_limit);

    // 4. Verify failure with JSON-RPC error -32602 (Agave #6845)
    match result {
        Err(RpcError::InvalidParams { code, message }) => {
            assert_eq!(code, -32602);
            assert!(
                message.contains("leader schedule for epoch 2 is unavailable"),
                "Expected error message for epoch 2, got: {message}"
            );
        }
        Ok(_) => panic!("Expected get_slot_leaders to fail with -32602 for missing Epoch 2"),
    }
}

#[tokio::test]
async fn test_precacher_triggers_100_slots_before_boundary() {
    let slots_per_epoch = 200;
    let epoch_schedule = EpochSchedule::custom(slots_per_epoch, slots_per_epoch, false);
    let (val_entries, _) = create_test_validators();

    let stake_provider = Arc::new(
        MockStakeDataProvider::new()
            .with_epoch_stakes(0, &val_entries)
            .with_epoch_stakes(1, &val_entries)
            .with_epoch_stakes(2, &val_entries)
    );

    let cache = Arc::new(LeaderScheduleCache::new(epoch_schedule, 1));
    let config = PreCacherConfig {
        warmup_slots_before_boundary: 100,
        max_concurrent_tasks: 2,
    };
    let pre_cacher = Arc::new(LeaderSchedulePreCacher::new(
        cache.clone(),
        stake_provider.clone(),
        config,
    ));

    // Warm Epoch 1 so only Epoch 2 is needed
    let vote_accounts_1 = stake_provider.get_epoch_vote_accounts(1).unwrap();
    let schedule_1 = LeaderSchedule::new(
        &vote_accounts_1,
        1,
        slots_per_epoch as usize,
        solana_leader_schedule::NUM_CONSECUTIVE_LEADER_SLOTS,
    );
    cache.warm_leader_schedule(1, Arc::new(schedule_1));

    // Epoch 1 spans slots 200..399. Last slot is 399.
    // 100 slots before boundary is slot 399 - 100 = 299.
    // Slot 298 should NOT trigger warming for Epoch 2
    assert_eq!(pre_cacher.should_warm_epoch(298), None);

    // Slot 299 MUST trigger warming for Epoch 2
    assert_eq!(pre_cacher.should_warm_epoch(299), Some(2));

    // Process slot 299
    let triggered = pre_cacher.on_slot_processed(299);
    assert!(triggered);

    // Wait for async background worker to complete
    let warmed = pre_cacher.wait_for_warmed_epoch(2, Duration::from_secs(2)).await;
    assert!(warmed, "Epoch 2 must be warmed in cache asynchronously");

    // Verify cache now contains Epoch 2
    assert!(cache.has_epoch(2));
    let schedule_2 = cache.get_epoch_leader_schedule(2).unwrap();
    assert_eq!(schedule_2.get_slot_leaders().count(), slots_per_epoch as usize);
}

#[tokio::test]
async fn test_rpc_seamless_across_boundary_with_precacher() {
    // End-to-end verification: Pre-cacher eliminates the -32602 error during 31-slot root lag
    let slots_per_epoch = 200;
    let epoch_schedule = EpochSchedule::custom(slots_per_epoch, slots_per_epoch, false);
    let (val_entries, _) = create_test_validators();

    let stake_provider = Arc::new(
        MockStakeDataProvider::new()
            .with_epoch_stakes(0, &val_entries)
            .with_epoch_stakes(1, &val_entries)
            .with_epoch_stakes(2, &val_entries)
    );

    let cache = Arc::new(LeaderScheduleCache::new(epoch_schedule, 1));
    let rpc_service = SlotLeaderRpcService::new(cache.clone());
    let pre_cacher = Arc::new(LeaderSchedulePreCacher::new(
        cache.clone(),
        stake_provider.clone(),
        PreCacherConfig::default(), // 100 slots warmup
    ));

    // Initialize Epoch 0 and 1
    pre_cacher.warm_epoch_sync(0).unwrap();
    pre_cacher.warm_epoch_sync(1).unwrap();

    // 100 slots before boundary (slot 299 in Epoch 1):
    pre_cacher.on_slot_processed(299);
    let warmed = pre_cacher.wait_for_warmed_epoch(2, Duration::from_secs(2)).await;
    assert!(warmed);

    // Now simulate the exact 31-slot root lag window at the epoch boundary (tip = 400..431)
    // Root is lagging 31 slots behind: root_slot = 400 - 31 = 369 (Epoch 1)
    for tip_slot in 400..=431 {
        let root_slot = tip_slot - 31;
        cache.set_root_slot(root_slot);

        // TPU client requests 100 leaders starting from tip_slot across boundary
        let leaders = rpc_service.get_slot_leaders(tip_slot, 100);
        assert!(
            leaders.is_ok(),
            "get_slot_leaders failed at tip_slot {} with root_slot {}: {:?}",
            tip_slot, root_slot, leaders.err()
        );
        let leaders_vec = leaders.unwrap();
        assert_eq!(leaders_vec.len(), 100);
    }

    // Also verify boundary span query (350..450) that previously failed with -32602
    let boundary_query = rpc_service.get_slot_leaders(350, 100).unwrap();
    assert_eq!(boundary_query.len(), 100);

    // Verify telemetry
    let stats = pre_cacher.stats();
    assert_eq!(stats.triggered_count.load(std::sync::atomic::Ordering::Relaxed), 1);
    assert_eq!(stats.warmed_count.load(std::sync::atomic::Ordering::Relaxed), 3); // epoch 0, 1, 2
}

#[tokio::test]
async fn test_precacher_deduplication_under_concurrency() {
    let slots_per_epoch = 200;
    let epoch_schedule = EpochSchedule::custom(slots_per_epoch, slots_per_epoch, false);
    let (val_entries, _) = create_test_validators();

    let stake_provider = Arc::new(
        MockStakeDataProvider::new()
            .with_epoch_stakes(0, &val_entries)
            .with_epoch_stakes(1, &val_entries)
    );

    let cache = Arc::new(LeaderScheduleCache::new(epoch_schedule, 0));
    let pre_cacher = Arc::new(LeaderSchedulePreCacher::new(
        cache.clone(),
        stake_provider.clone(),
        PreCacherConfig::default(),
    ));

    // Rapidly process slots 100..199 (all within the 100-slot warmup window for Epoch 1)
    let mut handles = Vec::new();
    for slot in 100..200 {
        let pc = pre_cacher.clone();
        handles.push(tokio::spawn(async move {
            pc.on_slot_processed(slot);
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    // Wait for warming
    let warmed = pre_cacher.wait_for_warmed_epoch(1, Duration::from_secs(2)).await;
    assert!(warmed);

    // Verify deduplication: despite 100 slot ticks, Epoch 1 was dispatched at most once
    let stats = pre_cacher.stats();
    let triggered = stats.triggered_count.load(std::sync::atomic::Ordering::Relaxed);
    let dedup = stats.deduplicated_count.load(std::sync::atomic::Ordering::Relaxed);
    let already_cached = stats.already_cached_count.load(std::sync::atomic::Ordering::Relaxed);

    assert_eq!(triggered, 1, "Only 1 background compute task must be triggered");
    assert_eq!(dedup + already_cached, 99, "All subsequent 99 ticks must be deduplicated");
}

#[test]
fn test_cache_lru_retention_policy() {
    let slots_per_epoch = 200;
    let epoch_schedule = EpochSchedule::custom(slots_per_epoch, slots_per_epoch, false);
    let (val_entries, _) = create_test_validators();

    let mut stake_builder = MockStakeDataProvider::new();
    for e in 0..15 {
        stake_builder = stake_builder.with_epoch_stakes(e, &val_entries);
    }
    let stake_provider = Arc::new(stake_builder);

    let cache = Arc::new(LeaderScheduleCache::new(epoch_schedule, 0));
    let pre_cacher = LeaderSchedulePreCacher::new(
        cache.clone(),
        stake_provider.clone(),
        PreCacherConfig::default(),
    );

    // Warm epochs 0 through 11 (12 epochs). MAX_SCHEDULES is 10.
    for e in 0..12 {
        pre_cacher.warm_epoch_sync(e).unwrap();
    }

    let guard = cache.cached_schedules.read();
    let (ref map, ref order) = *guard;
    assert_eq!(map.len(), 10, "Cache must not exceed MAX_SCHEDULES (10)");
    assert_eq!(order.len(), 10);

    // Epochs 0 and 1 should have been evicted by LRU
    assert!(!map.contains_key(&0));
    assert!(!map.contains_key(&1));
    // Epochs 2..=11 should be retained
    for e in 2..=11 {
        assert!(map.contains_key(&e));
    }
}

#[tokio::test]
async fn test_precacher_missing_stakes_graceful_handling() {
    let slots_per_epoch = 200;
    let epoch_schedule = EpochSchedule::custom(slots_per_epoch, slots_per_epoch, false);
    // Provider has NO stakes for Epoch 2
    let stake_provider = Arc::new(MockStakeDataProvider::new());

    let cache = Arc::new(LeaderScheduleCache::new(epoch_schedule, 1));
    let pre_cacher = Arc::new(LeaderSchedulePreCacher::new(
        cache.clone(),
        stake_provider,
        PreCacherConfig::default(),
    ));

    // Process slot 299 (in Epoch 1 boundary window)
    pre_cacher.on_slot_processed(299);

    // Wait a brief duration for the background task to conclude
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Cache should not have epoch 2
    assert!(!cache.has_epoch(2));

    // Inflight flag must be cleared so retry can happen
    assert!(!pre_cacher.is_inflight(2));

    // Telemetry must record missing stakes
    assert_eq!(
        pre_cacher.stats().missing_stakes_count.load(std::sync::atomic::Ordering::Relaxed),
        1
    );
}

#[tokio::test]
async fn test_next_leader_slot_continuity_across_boundary() {
    let slots_per_epoch = 200;
    let epoch_schedule = EpochSchedule::custom(slots_per_epoch, slots_per_epoch, false);
    let mut shared_vote_accounts = VoteAccountsHashMap::default();
    for _ in 0..5 {
        let va = VoteAccount::new_random();
        shared_vote_accounts.insert(*va.node_pubkey(), (1_000_000_000, va));
    }

    let stake_provider = Arc::new(
        MockStakeDataProvider::new()
            .with_shared_vote_accounts(0, shared_vote_accounts.clone())
            .with_shared_vote_accounts(1, shared_vote_accounts.clone())
            .with_shared_vote_accounts(2, shared_vote_accounts)
    );

    let cache = Arc::new(LeaderScheduleCache::new(epoch_schedule, 1));
    let pre_cacher = Arc::new(LeaderSchedulePreCacher::new(
        cache.clone(),
        stake_provider,
        PreCacherConfig::default(),
    ));

    pre_cacher.warm_epoch_sync(0).unwrap();
    pre_cacher.warm_epoch_sync(1).unwrap();

    // Trigger pre-cacher to warm Epoch 2 at slot 300
    pre_cacher.on_slot_processed(300);
    assert!(pre_cacher.wait_for_warmed_epoch(2, Duration::from_secs(2)).await);

    // Pick a validator that actually exists in the computed leader schedule
    let schedule_1 = cache.get_epoch_leader_schedule(1).unwrap();
    let target_validator = schedule_1.get_slot_leaders().next().unwrap().id;

    // Query next leader slot for target_validator starting from slot 390
    let next_slot = cache.next_leader_slot(&target_validator, 390, 200);
    assert!(next_slot.is_some(), "Next leader slot must be found across boundary");
    let (first, last) = next_slot.unwrap();
    assert!(first > 390);
    assert!(last >= first);
}

