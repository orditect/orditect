# Changelog

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