# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

