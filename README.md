# Solana Epoch-Boundary Leader Schedule Pre-Cacher

## Eliminating the 31-Slot RPC Dead Zone & Transaction Misrouting

[![GitHub](https://img.shields.io/badge/GitHub-coad1024--cmd%2Fsolana--leader--precached-blue)](https://github.com/coad1024-cmd/solana-leader-precached)
[![Subsystem](https://img.shields.io/badge/Subsystem-RPC%20%2F%20Ledger%20%2F%20TPU-green)](#)
[![Status](https://img.shields.io/badge/Status-VG--01%20Root%20Cause%20Analysis-orange)](#)

---

### 1. The Core Engineering Problem

In Solana's consensus model, leader schedules are deterministic functions of epoch stake distributions and PoH seed generation. Because stake states freeze at epoch boundaries, the schedule for epoch $N+1$ can be known in advance.

However, in the Agave validator and RPC nodes:
1. `LeaderScheduleCache::get_epoch_leader_schedule` relies on synchronous on-demand computation triggered upon crossing slot boundaries into epoch $N+1$.
2. When client applications, TPU clients, or MEV searchers query `get_slot_leaders` or `get_epoch_schedule` during the transition window, the cache is cold.
3. For **~31 slots (~12.4 seconds)**, the RPC endpoint returns:
   ```text
   RPC response error -32602: leader schedule for epoch YYY is unavailable
   ```
4. As a result, TPU clients fallback to stale leader schedules, misrouting block data and vote packets to past leaders, leading to elevated transaction drops and skipped slots at every epoch boundary.

---

### 2. Upstream Forensic Context

* **Agave Issue:** [#6845](https://github.com/anza-xyz/agave/issues/6845) (get_slot_leaders RPC method fails at epoch boundaries with error -32602)
* **Agave PR:** [#7765](https://github.com/anza-xyz/agave/pull/7765) (Initial attempts to loosen slot constraints)
* **Source Subsystems:**
  * `ledger/src/leader_schedule_cache.rs`
  * `runtime/src/bank.rs` (`epoch_stakes`, `get_leader_schedule_epoch`)
  * `rpc/src/rpc.rs` (`get_slot_leaders`)

---

### 3. The Solution: Asynchronous Pre-Computation Worker

Implement an asynchronous background warming task within `LeaderScheduleCache`:
1. **Trigger Window:** At slot $S = \text{epoch\_end} - 100$ (approx. 40 seconds prior to epoch transition).
2. **Read-Only Snapshot:** Fetch stake distribution from the frozen parent bank stakes.
3. **Async Generation:** Offload `LeaderSchedule::new_from_schedule` generation to a background thread pool (`rayon` or `tokio`).
4. **Zero-Lock Warming:** Insert the prepared schedule into `LeaderScheduleCache` prior to slot 0 of epoch $N+1$.

**Consensus Impact:** **Zero consensus risk.** Purely node-local optimization that guarantees instantaneous $O(1)$ cache hits for RPC queries and TPU routing.

---

### 4. Repository Structure

* `src/`: Standalone pre-cacher worker crate and cache wrapper.
* `tests/`: Epoch boundary transition simulator reproducing error -32602 and validating async warm-up.
* `patches/`: Clean git patch drop-in for `anza-xyz/agave` `ledger/src/leader_schedule_cache.rs`.

---

### 5. Verification Gates

- [x] **VG-00: Code Audit** - Isolated `LeaderScheduleCache` call path and reproducible test failure.
- [ ] **VG-01: Unit Test Harness** - Synthetic epoch transition test proving elimination of -32602 errors.
- [ ] **VG-02: Lock Contention Profiling** - Ensure pre-caching worker does not introduce RwLock contention on main replay thread.
- [ ] **VG-03: Upstream PR** - Formal PR submission to `anza-xyz/agave`.
