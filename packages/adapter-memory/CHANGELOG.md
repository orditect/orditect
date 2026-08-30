# Changelog

## [0.1.5] - TBD

### Fixed
- `save()` terminal guard: only a non-empty, differing status is a state
  mutation — an empty status (absence of intent) no longer raises
  TerminalStateViolationError (was a false rejection).
- Merge logic unified to `mechanism.fold_snapshot_rows` (single executable
  definition; created_at no longer drifts on merge).
- Sorting by `expire_at` no longer raises TypeError on mixed
  None/datetime — no-expiry sorts as infinitely far (ASC last, DESC first).

### Tests
- New `TestSparseSaveSemantics` (CF-SNP-014 mirror) and `TestExpireAtSort`
  (CF-SNP-015 mirror).

## [0.1.4] - TBD

### Fixed
- T4 idempotency: `save_terminal` and `append` now compare business
  content excluding mechanism clock fields via
  `mechanism.idempotent_payload_equal` — a reconstructed retry with a new
  producer timestamp is a silent dedup, not a conflict.
- T3 second face: `save` now merges non-state fields to complete the
  record (parent_task_id, pointers, error, cost, model, expire_at) — a
  sparse same-generation save no longer erases previously recorded
  fields; status advances only with a non-empty incoming value.

### Tests
- New `test_idempotency_semantics.py`: reconstructed-retry dedup pins.
- New `TestNonStateMerge`: CF-SNP-013 mirror.

## [0.1.3] - TBD

### Changed
- Version alignment with ecosystem (additive only, no behavior change).
- Dependency floor check: protocol imports predate 0.1.3; floor stays >=0.1,<0.2.

## [0.1.2] - TBD

### Added
- MemoryDependencyPart (fifth domain part); MemoryStore now composes five
  parts. Passes CF-DEP-001..006.

### Changed
- **Behavior change**: out-of-whitelist sort.field / group_by now raise
  InvalidQueryError (was silent getattr fallback).
- Audit part uses created_at (model rename).