# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.6] - TBD

### Fixed
- `task_reopen.lua` now also clears `progress` / `cancel_outcome` —
  generation-scoped settle metadata must not leak into a new generation
  (same rationale as the v0.1.5 result/error clear). `lua_contract.md`
  updated. New pinning test `test_reopen_clears_progress_and_cancel_outcome`.
- Shielded release tasks in `SemaphoreHold` / `@limited` are now
  strong-referenced — an orphaned shield task is never GC-collected
  mid-release.

### Changed
- Version alignment with ecosystem (no behavior change).

## [0.1.5] - TBD

### Fixed
- `task_reopen.lua` now clears the old generation's `result`/`error` — a
  new generation must not inherit the previous generation's output
  (prevents a crash between reopen and re-execution from being misjudged
  as REUSE by resume).
- `quota_release.lua`: a partial release with pttl<=0 (integer-second TTL
  truncation in the key's final second) no longer leaves an eternal
  non-zero pending key — a fallback TTL is applied (new ARGV[2]).
- `quota_reserve.lua`: the reaping pass no longer writes into a dead
  pending key (which suppressed the renewal rebuild); a dead counter with
  surviving leases is rebuilt from the leases before any quota decision,
  on both the renewal and fresh-reserve paths (over-admission fix).
- `_sync_attached_ttl` adjudicated final semantics: an attached key that
  EXISTS follows the owner's TTL (or a fallback when the owner is gone —
  its data is real business state and must not be dropped); a key that
  does NOT exist is never materialized (genuine ghosts never pollute
  SCAN-based queries). (2 FLIPs in pinning tests)

### Changed
- `docs/lua_contract.md`: reaping window semantics documented (the reaping
  pass expires leases against the CURRENT call's task_ttl, uniformly
  across a scope — callers must use a consistent task_ttl_sec per scope);
  task_reopen / quota_reserve / quota_release sections updated.

## [0.1.4] - TBD

### Fixed
- `quota_reserve.lua` idempotent-renewal TTL gap: a renewal chain (>= 2
  renewals, each within task_ttl) could push a lease's logical lifetime
  past `pending_key`'s fallback TTL; the key dying while a live lease
  remained evaporated the lease's units from the counter and let a later
  reserve over-admit. The idempotent branch now bumps `pending_key`'s
  TTL (or rebuilds it from surviving leases when already dead).
- `quota_release.lua`: releasing to zero with no TTL now `DEL`s
  `pending_key` instead of leaving an eternal "0" key.
- `_sync_attached_ttl` falls back to `default_expire_time` when the owner
  hot record is already gone — a ghost counter created by DECR on a
  missing key is never eternal.

### Changed
- `docs/lua_contract.md`: rewritten in English; preserve-mode TTL
  precision boundary documented (integer-second rounding; index member
  may be lazily cleaned up to <1s before the primary record); legacy
  version references consolidated.
- `docs/design_decisions.md`: rewritten for orditect (legacy taskbase
  handover content removed; DD-001..DD-010 retained with rationale).
- `limiter/semaphore.py`: fixed `_is_match`'s docstring (was a
  copy-paste of `acquire`'s).
- `redis/task_db.py`: removed the dead constructor-time
  `default_task_data["timestamp"]` value.

## [0.1.3] - TBD

### Changed
- Version alignment with ecosystem (additive only, no behavior change).
- Dependency floor check: imports from orditect-protocol predate 0.1.3; floor stays >=0.1,<0.2.

## [0.1.2] - TBD

### Changed
- Hot-record `_now_str()` produces UTC-aware ISO timestamps (T7 alignment;
  field name "timestamp" unchanged — mechanism-internal, exempt from the
  protocol wire-format vocabulary).

### Compatibility notes
- Dependency floor check: imports from orditect-protocol (TaskPointer)
  predate 0.1.2; floor stays >=0.1,<0.2.

## [0.1.1] - TBD

### Added
- Dependency-governance primitives on TaskRedisDB (v0.1.1, plain Redis
  commands — zero Lua changes, lua_contract.md untouched):
  - active-children notification set: sadd_active_child / srem_active_child
    / get_active_children
  - remaining-deps counter: set_remaining_deps / decr_remaining_deps /
    get_remaining_deps (DECR on a missing key yields -1 — tolerated)
  - ready scan: list_ready_dep_tasks (SCAN-based, optional caller-injected
    status filter — vocabulary-neutral, T6)
  - cancel-vote set: vote_and_check_threshold (SADD + SCARD in one
    MULTI/EXEC transaction — exactly one concurrent voter observes the
    threshold), get_cancel_votes, clear_cancel_votes
  - result-consumer dedup: sadd_result_consumer (True = first time)
- Attached-key TTL discipline: every dependency key lives under the owning
  task's hot-record key, expires at the same instant, and follows the hot
  record's TTL on update_task (best-effort, logged only).

### Changed
- (none — pure addition; zero behavior change for callers that do not use
  the new methods)

## [0.1.0] - TBD

### Added
- Renamed from fastapi-taskbase to orditect-core (namespace package migration).
- Dependency on orditect-protocol (TaskPointer model reuse, T5/T11 term alignment).
- reopen_task primitive (task_reopen.lua, 10th Lua script): controlled
  new-generation opening for terminal tasks, enabling resume/rerun without
  violating terminal-state protection (T3). execution_id hot-path projection
  aligned with flow and protocol snapshot domain (T11). Concurrent reopen
  exactly one winner (T4/T10); old generation traced in
  previous_execution_ids (capped at 50). 
- initialize_task assigns an initial execution_id at creation (task_init.lua
  ARGV[7]): tasks carry a generation identity from birth (T11 hot-path
  projection); reopen_task only advances it. Idempotent skip preserves the
  existing execution_id.
### Changed
- (none — zero behavior change commitment)

### Removed
- docs/taskstore_backlog.md moved to commercial adapter project
  (contract-level content absorbed by orditect-protocol terms.md).

### Design Decision
- Governance hot path (semaphore / quota / task state) permanently pinned to
  Redis, not abstracted into protocol (millisecond-sensitive, Lua-atomic).
- Content plane (pointer-ized fields) reuses TaskPointer model from
  orditect-protocol but does not call storage domain interfaces (content
  access is the responsibility of flow / commercial adapters).

