use {
    crate::leader_schedule_cache::LeaderScheduleCache,
    log::*,
    parking_lot::RwLock,
    solana_clock::{Epoch, Slot},
    solana_leader_schedule::{LeaderSchedule, NUM_CONSECUTIVE_LEADER_SLOTS},
    solana_vote::vote_account::VoteAccountsHashMap,
    std::{
        collections::HashSet,
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc,
        },
        time::{Duration, Instant},
    },
    thiserror::Error,
};

/// Default lookahead window: 100 slots before the epoch boundary.
/// This matches `MAX_FANOUT_SLOTS = 100` from `tpu_client`, ensuring that as soon as
/// TPU client or RPC callers begin fanout lookahead towards the upcoming epoch,
/// the upcoming epoch's leader schedule is already computed and warmed in cache.
pub const DEFAULT_WARMUP_SLOTS_BEFORE_BOUNDARY: u64 = 100;

#[derive(Error, Debug)]
pub enum PreCacherError {
    #[error("Stake data missing for epoch {0}")]
    MissingStakeData(Epoch),
    #[error("Leader schedule calculation failed for epoch {0}")]
    CalculationFailed(Epoch),
    #[error("Epoch {0} calculation timed out")]
    Timeout(Epoch),
}

/// Provider abstraction for epoch vote accounts and stakes.
pub trait StakeDataProvider: Send + Sync {
    /// Retrieves vote accounts map for the given epoch.
    fn get_epoch_vote_accounts(&self, epoch: Epoch) -> Option<VoteAccountsHashMap>;
}

/// Configuration for the asynchronous pre-cacher.
#[derive(Debug, Clone)]
pub struct PreCacherConfig {
    /// Number of slots before the epoch boundary to trigger pre-caching (default: 100).
    pub warmup_slots_before_boundary: u64,
    /// Maximum concurrent pre-caching tasks.
    pub max_concurrent_tasks: usize,
}

impl Default for PreCacherConfig {
    fn default() -> Self {
        Self {
            warmup_slots_before_boundary: DEFAULT_WARMUP_SLOTS_BEFORE_BOUNDARY,
            max_concurrent_tasks: 4,
        }
    }
}

/// Performance and operational telemetry for the pre-cacher.
#[derive(Default, Debug)]
pub struct PreCacherStats {
    pub triggered_count: AtomicU64,
    pub warmed_count: AtomicU64,
    pub deduplicated_count: AtomicU64,
    pub already_cached_count: AtomicU64,
    pub missing_stakes_count: AtomicU64,
    pub total_compute_duration_us: AtomicU64,
}

/// Asynchronous Epoch-Boundary Leader Schedule Pre-Cacher.
///
/// Operates off the consensus critical path. Monitors slot progression and initiates background
/// schedule computation 100 slots prior to the epoch boundary, eliminating the 31-slot gap
/// where `get_slot_leaders` would otherwise return RPC error -32602.
pub struct LeaderSchedulePreCacher {
    cache: Arc<LeaderScheduleCache>,
    stake_provider: Arc<dyn StakeDataProvider>,
    config: PreCacherConfig,
    inflight_epochs: Arc<RwLock<HashSet<Epoch>>>,
    stats: Arc<PreCacherStats>,
}

impl LeaderSchedulePreCacher {
    pub fn new(
        cache: Arc<LeaderScheduleCache>,
        stake_provider: Arc<dyn StakeDataProvider>,
        config: PreCacherConfig,
    ) -> Self {
        Self {
            cache,
            stake_provider,
            config,
            inflight_epochs: Arc::new(RwLock::new(HashSet::new())),
            stats: Arc::new(PreCacherStats::default()),
        }
    }

    pub fn stats(&self) -> &Arc<PreCacherStats> {
        &self.stats
    }

    pub fn config(&self) -> &PreCacherConfig {
        &self.config
    }

    /// Evaluates whether the given `current_slot` requires warming the upcoming epoch's schedule.
    /// Returns `Some(target_epoch)` if pre-caching should be initiated.
    pub fn should_warm_epoch(&self, current_slot: Slot) -> Option<Epoch> {
        let epoch_schedule = &self.cache.epoch_schedule;
        let (current_epoch, _slot_index) = epoch_schedule.get_epoch_and_slot_index(current_slot);
        let last_slot_in_epoch = epoch_schedule.get_last_slot_in_epoch(current_epoch);

        // Trigger warming exactly within the warmup threshold before the epoch boundary
        let threshold_slot = last_slot_in_epoch.saturating_sub(self.config.warmup_slots_before_boundary);
        if current_slot >= threshold_slot {
            let next_epoch = current_epoch.saturating_add(1);
            if !self.cache.has_epoch(next_epoch) {
                return Some(next_epoch);
            }
        }

        None
    }

    pub fn is_inflight(&self, epoch: Epoch) -> bool {
        self.inflight_epochs.read().contains(&epoch)
    }

    /// Non-blocking callback invoked on slot processing.
    /// If within the warmup window, dispatches background calculation without blocking the caller.
    /// Safely handles deduplication across concurrent slot notifications.
    pub fn on_slot_processed(&self, current_slot: Slot) -> bool {
        let epoch_schedule = &self.cache.epoch_schedule;
        let (current_epoch, _slot_index) = epoch_schedule.get_epoch_and_slot_index(current_slot);
        let last_slot_in_epoch = epoch_schedule.get_last_slot_in_epoch(current_epoch);
        let threshold_slot = last_slot_in_epoch.saturating_sub(self.config.warmup_slots_before_boundary);

        if current_slot < threshold_slot {
            return false;
        }

        let target_epoch = current_epoch.saturating_add(1);
        if self.cache.has_epoch(target_epoch) {
            self.stats.already_cached_count.fetch_add(1, Ordering::Relaxed);
            return false;
        }

        {
            let mut inflight = self.inflight_epochs.write();
            if self.cache.has_epoch(target_epoch) {
                self.stats.already_cached_count.fetch_add(1, Ordering::Relaxed);
                return false;
            }
            if !inflight.insert(target_epoch) {
                self.stats.deduplicated_count.fetch_add(1, Ordering::Relaxed);
                trace!("Epoch {} calculation already inflight, skipping duplicate dispatch", target_epoch);
                return false;
            }
        }

        self.dispatch_pre_cache_task(target_epoch);
        true
    }

    /// Spawns background calculation for the target epoch.
    pub fn dispatch_pre_cache_task(&self, target_epoch: Epoch) {
        self.stats.triggered_count.fetch_add(1, Ordering::Relaxed);
        let cache = self.cache.clone();
        let stake_provider = self.stake_provider.clone();
        let inflight_epochs = self.inflight_epochs.clone();
        let stats = self.stats.clone();

        tokio::spawn(async move {
            info!("Starting asynchronous pre-cache calculation for epoch {}", target_epoch);
            let start = Instant::now();

            let cache_for_compute = cache.clone();
            let compute_result = tokio::task::spawn_blocking(move || {
                let vote_accounts = stake_provider.get_epoch_vote_accounts(target_epoch)?;
                let slots_in_epoch = cache_for_compute.epoch_schedule.get_slots_in_epoch(target_epoch) as usize;

                let schedule = LeaderSchedule::new(
                    &vote_accounts,
                    target_epoch,
                    slots_in_epoch,
                    NUM_CONSECUTIVE_LEADER_SLOTS,
                );
                Some(Arc::new(schedule))
            })
            .await;

            // Remove from inflight set regardless of outcome
            inflight_epochs.write().remove(&target_epoch);

            match compute_result {
                Ok(Some(leader_schedule)) => {
                    let elapsed_us = start.elapsed().as_micros() as u64;
                    stats.total_compute_duration_us.fetch_add(elapsed_us, Ordering::Relaxed);
                    cache.warm_leader_schedule(target_epoch, leader_schedule);
                    stats.warmed_count.fetch_add(1, Ordering::Relaxed);
                    info!(
                        "Successfully pre-cached and warmed epoch {} in {} us",
                        target_epoch, elapsed_us
                    );
                }
                Ok(None) => {
                    stats.missing_stakes_count.fetch_add(1, Ordering::Relaxed);
                    warn!(
                        "Pre-caching failed for epoch {}: vote accounts unavailable",
                        target_epoch
                    );
                }
                Err(err) => {
                    error!("Pre-caching panic/join error for epoch {}: {:?}", target_epoch, err);
                }
            }
        });
    }

    /// Synchronous helper for testing or foreground execution.
    pub fn warm_epoch_sync(&self, target_epoch: Epoch) -> Result<(), PreCacherError> {
        if self.cache.has_epoch(target_epoch) {
            self.stats.already_cached_count.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }

        let vote_accounts = self
            .stake_provider
            .get_epoch_vote_accounts(target_epoch)
            .ok_or(PreCacherError::MissingStakeData(target_epoch))?;

        let slots_in_epoch = self.cache.epoch_schedule.get_slots_in_epoch(target_epoch) as usize;
        let schedule = LeaderSchedule::new(
            &vote_accounts,
            target_epoch,
            slots_in_epoch,
            NUM_CONSECUTIVE_LEADER_SLOTS,
        );

        self.cache.warm_leader_schedule(target_epoch, Arc::new(schedule));
        self.stats.warmed_count.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Asynchronously waits until the given epoch is present in the cache, or returns timeout.
    pub async fn wait_for_warmed_epoch(&self, epoch: Epoch, timeout_dur: Duration) -> bool {
        let deadline = Instant::now() + timeout_dur;
        while Instant::now() < deadline {
            if self.cache.has_epoch(epoch) {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        self.cache.has_epoch(epoch)
    }
}
