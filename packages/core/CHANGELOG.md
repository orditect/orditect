# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

