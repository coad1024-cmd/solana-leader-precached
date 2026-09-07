//! Asynchronous Epoch-Boundary Leader Schedule Pre-Cacher
//! Reference: Agave Issue #6845 & Priority #3 Architecture
//!
//! This crate provides:
//! 1. Audited, pre-warmable `LeaderScheduleCache` matching Agave consensus semantics.
//! 2. `LeaderSchedulePreCacher`: An asynchronous worker that calculates and warms the upcoming
//!    epoch leader schedule 100 slots before the epoch boundary, eliminating the 31-slot
//!    -32602 `Invalid slot range` JSON-RPC error caused by Tower BFT 32-lockout root lag.
//! 3. `SlotLeaderRpcService`: Simulation and verification of RPC `get_slot_leaders` behavior.

pub mod leader_schedule_cache;
pub mod pre_cacher;
pub mod rpc_service;

pub use leader_schedule_cache::LeaderScheduleCache;
pub use pre_cacher::{LeaderSchedulePreCacher, PreCacherConfig, StakeDataProvider};
pub use rpc_service::{RpcError, RpcResult, SlotLeaderRpcService};
