# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.6] - TBD

### Fixed
- `StreamResult` deduplicated — `runner/stream.py` no longer re-defines the
  dataclass it also imports; executors now return the canonical
  `stream.stream_result.StreamResult` (isinstance-safe across the
  runner/finalizer boundary). New pin.
- `tests/integration/__init__.py` no longer holds dead test cases (pytest
  never collects them there).

### Changed
- `TaskflowResultStore` documents `manifest` as a reserved status word for
  its task records (callers must not drive status transitions on them).
- Dependency floors raised to >=0.1.6.

## [0.1.5] - TBD

### Changed
- Version alignment with ecosystem (additive only, no behavior change).
- Dependency floor raised: orditect-core>=0.1.5, orditect-flow>=0.1.5,
  orditect-protocol>=0.1.5.

## [0.1.4] - TBD

### Fixed
- `StreamRunner.cancel()` no longer blocks on a full mux queue: the
  cancelled event is best-effort (cancel state is authoritative in the
  token); the control path never waits on the data path.

### Changed
- `client/resolver.py`: status words are now caller-injectable
  (`success_words` / `terminal_words`), defaulting to the flow vocabulary
  (T6; was hardcoded).

### Tests
- `test_stream_break_marks_cancelled_and_pointerizes_partial`: the audit
  write lands one event-loop tick after `break`; the test now yields
  before asserting (was timing-flaky).

### Hygiene
- Removed the mistakenly packaged `src/orditect/stream/tests/` directory
  (was shipped inside the wheel)

## [0.1.3] - TBD

### Changed
- Version alignment with ecosystem (additive only, no behavior change).
- Dependency floor check: protocol imports predate 0.1.3; floor stays >=0.1,<0.2.

## [0.1.2] - TBD

### Changed
- Version bump only (no functional change; keeps the ecosystem version line
  aligned). Dependency floor check: protocol imports predate 0.1.2; floor
  stays >=0.1,<0.2.

## [0.1.0] - TBD

First release as orditect-stream (renamed from fastapi-taskstream).

### Added
- Renamed from fastapi-taskstream to orditect-stream (namespace package
  migration).
- Dependencies on orditect-core, orditect-flow, orditect-protocol.
- **Result domain relocation (S2)**: ProtocolResultStore thin adapter +
  get_protocol_store() entry point — the stream ResultStoreProtocol is
  relocated onto the orditect-protocol result domain (ResultWriter/Reader).
  ttl seconds are converted to an absolute expire_at at save time (T1/T7);
  get returns None once expired (lazy expiry, T1). Backward compatible:
  the stream-facing signature (ttl seconds) is unchanged, and the built-in
  MemoryResultStore is preserved (relocation as an 接入点, not a replacement).

### Changed
- (none — zero behavior change commitment)

### Design Decision
- Pause/resume streaming semantics deferred to v0.2.0 (decided with the
  flow suspend mechanism): on pause the stream ends with a manifest
  persisted; resume opens a new stream (aligned with the new execution
  generation). Documented in docs/protocol.md.