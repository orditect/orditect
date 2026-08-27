# orditect-adapter-ui Changelog

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