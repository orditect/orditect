# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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