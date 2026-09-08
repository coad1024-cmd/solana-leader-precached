use {
    itertools::Itertools,
    log::*,
    parking_lot::RwLock,
    solana_clock::{Epoch, Slot},
    solana_epoch_schedule::EpochSchedule,
    solana_leader_schedule::{FixedSchedule, LeaderSchedule, SlotLeader},
    solana_pubkey::Pubkey,
    std::{
        collections::{hash_map::Entry, HashMap, VecDeque},
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc,
        },
    },
};

pub type CachedSchedules = (HashMap<Epoch, Arc<LeaderSchedule>>, VecDeque<u64>);
pub const MAX_SCHEDULES: usize = 10;

#[derive(Debug, Clone, Copy)]
pub struct CacheCapacity(pub usize);
impl Default for CacheCapacity {
    fn default() -> Self {
        CacheCapacity(MAX_SCHEDULES)
    }
}

/// Audited `LeaderScheduleCache` maintaining in-memory leader schedules for confirmed and warmed epochs.
pub struct LeaderScheduleCache {
    /// Map from an epoch to a leader schedule for that epoch, along with LRU eviction order
    pub cached_schedules: RwLock<CachedSchedules>,
    pub epoch_schedule: EpochSchedule,
    pub max_epoch: AtomicU64,
    pub max_schedules: CacheCapacity,
    pub fixed_schedule: Option<Arc<FixedSchedule>>,
}

impl Default for LeaderScheduleCache {
    fn default() -> Self {
        Self {
            cached_schedules: RwLock::new((HashMap::new(), VecDeque::new())),
            epoch_schedule: EpochSchedule::without_warmup(),
            max_epoch: AtomicU64::new(0),
            max_schedules: CacheCapacity::default(),
            fixed_schedule: None,
        }
    }
}

impl LeaderScheduleCache {
    pub fn new(epoch_schedule: EpochSchedule, max_epoch: Epoch) -> Self {
        Self {
            cached_schedules: RwLock::new((HashMap::new(), VecDeque::new())),
            epoch_schedule,
            max_epoch: AtomicU64::new(max_epoch),
            max_schedules: CacheCapacity::default(),
            fixed_schedule: None,
        }
    }

    pub fn max_schedules(&self) -> usize {
        self.max_schedules.0
    }

    pub fn max_epoch(&self) -> Epoch {
        self.max_epoch.load(Ordering::Acquire)
    }

    /// Checks if a schedule for the given epoch is present in the cache.
    pub fn has_epoch(&self, epoch: Epoch) -> bool {
        self.cached_schedules.read().0.contains_key(&epoch)
    }

    /// Sets the root bank slot, updating `max_epoch` and evicting older schedules if needed.
    /// In the unpatched agave implementation, `set_root` only computes schedules when `new_max_epoch > old_max_epoch`.
    pub fn set_root_slot(&self, root_slot: Slot) -> Epoch {
        let new_max_epoch = self.epoch_schedule.get_leader_schedule_epoch(root_slot);
        let old_max_epoch = self.max_epoch.fetch_max(new_max_epoch, Ordering::AcqRel);
        trace!(
            "set_root_slot: root_slot={}, old_max_epoch={}, new_max_epoch={}",
            root_slot, old_max_epoch, new_max_epoch
        );
        new_max_epoch
    }

    /// Warms the cache by inserting a pre-computed leader schedule for an epoch.
    /// This is called by the asynchronous pre-cacher worker before the epoch boundary.
    pub fn warm_leader_schedule(&self, epoch: Epoch, leader_schedule: Arc<LeaderSchedule>) -> bool {
        let mut guard = self.cached_schedules.write();
        let (ref mut cached_schedules, ref mut order) = *guard;

        match cached_schedules.entry(epoch) {
            Entry::Vacant(entry) => {
                entry.insert(leader_schedule);
                order.push_back(epoch);
                Self::retain_latest(cached_schedules, order, self.max_schedules());

                // Advance max_epoch if the warmed epoch is higher
                self.max_epoch.fetch_max(epoch, Ordering::AcqRel);
                debug!("Warmed leader schedule for epoch {}", epoch);
                true
            }
            Entry::Occupied(_) => {
                debug!("Leader schedule for epoch {} already present, warming skipped", epoch);
                false
            }
        }
    }

    pub fn slot_leader_at(&self, slot: Slot) -> Option<SlotLeader> {
        if self.epoch_schedule.slots_per_epoch == 0 {
            return None;
        }
        self.slot_leader_at_no_compute(slot)
    }

    pub fn slot_leader_at_no_compute(&self, slot: Slot) -> Option<SlotLeader> {
        let (epoch, slot_index) = self.epoch_schedule.get_epoch_and_slot_index(slot);
        if let Some(ref fixed_schedule) = self.fixed_schedule {
            return Some(fixed_schedule.leader_schedule[slot_index]);
        }
        self.cached_schedules
            .read()
            .0
            .get(&epoch)
            .map(|schedule| schedule[slot_index])
    }

    /// Slot leader query with unconfirmed epoch guard matching `LeaderScheduleCache::slot_leader_at_else_compute`.
    pub fn slot_leader_at_with_epoch_guard(&self, slot: Slot) -> Option<SlotLeader> {
        let bank_epoch = self.epoch_schedule.get_epoch_and_slot_index(slot).0;
        if bank_epoch > self.max_epoch.load(Ordering::Acquire) {
            debug!("Requested leader in slot: {slot} of unconfirmed epoch: {bank_epoch}");
            return None;
        }
        self.slot_leader_at_no_compute(slot)
    }

    pub fn get_epoch_leader_schedule(&self, epoch: Epoch) -> Option<Arc<LeaderSchedule>> {
        if let Some(ref fixed_schedule) = self.fixed_schedule {
            return Some(fixed_schedule.leader_schedule.clone());
        }
        self.cached_schedules.read().0.get(&epoch).cloned()
    }

    pub fn set_fixed_leader_schedule(&mut self, fixed_schedule: Option<FixedSchedule>) {
        self.fixed_schedule = fixed_schedule.map(Arc::new);
    }

    fn retain_latest(
        schedules: &mut HashMap<Epoch, Arc<LeaderSchedule>>,
        order: &mut VecDeque<u64>,
        max_schedules: usize,
    ) {
        while schedules.len() > max_schedules {
            if let Some(first) = order.pop_front() {
                schedules.remove(&first);
            }
        }
    }

    /// Finds the upcoming leader slot for `pubkey` after `current_slot`.
    pub fn next_leader_slot(
        &self,
        pubkey: &Pubkey,
        current_slot: Slot,
        max_slot_range: u64,
    ) -> Option<(Slot, Slot)> {
        let (epoch, start_index) = self.epoch_schedule.get_epoch_and_slot_index(current_slot + 1);
        let max_epoch = self.max_epoch.load(Ordering::Acquire);
        if epoch > max_epoch {
            debug!(
                "Requested next leader in slot: {} of unconfirmed epoch: {}",
                current_slot + 1,
                epoch
            );
            return None;
        }

        let schedules: Vec<_> = (epoch..=max_epoch)
            .map(|e| self.get_epoch_leader_schedule(e))
            .while_some()
            .zip(epoch..)
            .collect();

        let mut schedule = schedules
            .iter()
            .flat_map(|(leader_schedule, k)| {
                let offset = if *k == epoch { start_index as usize } else { 0 };
                let num_slots = self.epoch_schedule.get_slots_in_epoch(*k) as usize;
                let first_slot = self.epoch_schedule.get_first_slot_in_epoch(*k);
                leader_schedule
                    .get_leader_upcoming_slots(pubkey, offset)
                    .take_while(move |i| *i < num_slots)
                    .map(move |i| i as Slot + first_slot)
            });

        let first_slot = schedule.next()?;
        let max_slot = first_slot.saturating_add(max_slot_range);
        let last_slot = schedule
            .take_while(|slot| *slot < max_slot)
            .zip(first_slot + 1..)
            .take_while(|(a, b)| a == b)
            .map(|(s, _)| s)
            .last()
            .unwrap_or(first_slot);
        Some((first_slot, last_slot))
    }
}
