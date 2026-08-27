## [0.1.2] - TBD

### Added
- MemoryDependencyPart (fifth domain part); MemoryStore now composes five
  parts. Passes CF-DEP-001..006.

### Changed
- **Behavior change**: out-of-whitelist sort.field / group_by now raise
  InvalidQueryError (was silent getattr fallback).
- Audit part uses created_at (model rename).