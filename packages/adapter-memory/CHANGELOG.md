# Changelog

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