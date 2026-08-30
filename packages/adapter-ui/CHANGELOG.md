# orditect-adapter-ui Changelog

## [0.1.6] - TBD

### Fixed
- Query kwargs are strict: unknown keyword arguments now raise `TypeError`
  instead of being silently swallowed (T8 spirit). New pins.
- Mixed datetime forms are normalized (seed-injected datetime objects vs
  file-read ISO strings) so latest-generation folding/sorting stay
  coherent. New pin.

### Changed
- `MemoryActionQueue` receipts are bounded (`max_receipts`, default 10000)
  with LRU eviction; retention window semantics documented. New pins.
- Dependency floors raised to >=0.1.6.

## [0.1.5] - TBD

### Fixed
- `SnapshotView.query` now returns latest generations only (was: returned
  every generation, violating the SnapshotReader contract) and honors
  sort / page / time_range (was: silently swallowed — a T8 violation).
- `SnapshotView.aggregate` validates group_by against the mechanism
  whitelist (raises InvalidQueryError; was: silently accepted).
- `SnapshotView.get_tree` folds per (task_id, step) (was: per task_id —
  latent cross-step collision).
- `SnapshotView._fold` unified to `mechanism.fold_snapshot_rows` (single
  executable merge definition).
- `AuditView.query` now honors sort / page / time_range; out-of-whitelist
  sort raises InvalidQueryError.

### Tests
- New contract-alignment pins: latest-only query, out-of-whitelist
  sort/group_by rejection, audit sort/page/whitelist.

## [0.1.4] - TBD

### Fixed
- `SnapshotView.aggregate` now folds to the latest generation per node
  (task_id, step) — CF-VIEW-004 semantics; previously every generation
  contributed to the bucket counts.

### Tests
- `test_aggregate` flipped (FLIP): node 'a' with generations
  (e1=done, e2=running) now counts as running at its latest generation;
  done holds only 'root'.
- `test_idempotent_action_dedup` flipped (FLIP): the second pause on an
  already-terminal task is now correctly REJECTED (aligned with the flow
  not-found/terminal contract).

## [0.1.3] - TBD

### Added
- Initial release: UI adapter reference implementation.
- TraceBundleReader (consumer read): parses trace-bundle directories
  (ndjson envelope rows + JSON payloads) into protocol domain models,
  without importing orditect-core/flow internals. Supports seed() hook
  for conformance consumer profile.
- ActionSinkAdapter (action sink, command-queue form per DD-013):
  converts UI/HITL/MCP/agent calls into action commands enqueued for
  flow's ActionDispatcher. Action records double as audit events
  (event_id = action_id) for idempotency and traceability.
- MemoryActionQueue: in-memory action queue reference implementation
  (production should use hot-path Redis-backed queue).
- Passes consumer profile (TraceBundleReader) + action profile
  (ActionSinkAdapter + ActionDispatcher).