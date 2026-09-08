use {
    crate::leader_schedule_cache::LeaderScheduleCache,
    log::*,
    solana_clock::Slot,
    solana_pubkey::Pubkey,
    std::sync::Arc,
    thiserror::Error,
};

pub const RPC_INVALID_PARAMS_CODE: i32 = -32602;

#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum RpcError {
    #[error("RPC response error {code}: {message}")]
    InvalidParams { code: i32, message: String },
}

pub type RpcResult<T> = Result<T, RpcError>;

/// Simulates the Solana JSON-RPC `getSlotLeaders` handler as implemented in `solana-rpc/src/rpc.rs`.
/// Reproduces and audits the exact failure mode where missing epoch schedules trigger error -32602.
pub struct SlotLeaderRpcService {
    leader_schedule_cache: Arc<LeaderScheduleCache>,
}

impl SlotLeaderRpcService {
    pub fn new(leader_schedule_cache: Arc<LeaderScheduleCache>) -> Self {
        Self {
            leader_schedule_cache,
        }
    }

    /// Corresponds to `JsonRpcRequestProcessor::get_slot_leaders`.
    /// Traverses slots starting from `start_slot` across epoch boundaries.
    /// If an epoch's schedule is not cached, returns JSON-RPC -32602 (`InvalidParams`).
    pub fn get_slot_leaders(&self, start_slot: Slot, limit: usize) -> RpcResult<Vec<Pubkey>> {
        let epoch_schedule = &self.leader_schedule_cache.epoch_schedule;
        let (mut epoch, mut slot_index) = epoch_schedule.get_epoch_and_slot_index(start_slot);

        let mut slot_leaders = Vec::with_capacity(limit);
        while slot_leaders.len() < limit {
            if let Some(leader_schedule) =
                self.leader_schedule_cache.get_epoch_leader_schedule(epoch)
            {
                slot_leaders.extend(
                    leader_schedule
                        .get_slot_leaders()
                        .map(|slot_leader| slot_leader.id)
                        .skip(slot_index as usize)
                        .take(limit.saturating_sub(slot_leaders.len())),
                );
            } else {
                let msg = format!(
                    "Invalid slot range: leader schedule for epoch {epoch} is unavailable"
                );
                debug!("get_slot_leaders failed: {}", msg);
                return Err(RpcError::InvalidParams {
                    code: RPC_INVALID_PARAMS_CODE,
                    message: msg,
                });
            }

            epoch += 1;
            slot_index = 0;
        }

        Ok(slot_leaders)
    }
}
