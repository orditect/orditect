# orditect-adapter-local Changelog

## [0.1.3] - TBD

### Added
- Initial release: local-file storage adapter (document-family reference).
- Five domains over plain files:
  - content: content-addressed blobs (sha256/<aa>/<digest>) + metadata sidecar
  - audit: append-only ndjson envelope stream (audit.ndjson)
  - result: one JSON file per stream_id (results/<id>.json) with lazy expiry
  - snapshot: append-only ndjson envelope stream (snapshots.ndjson) with
    T3/T4/T1 semantics evaluated on read-path folding
  - dependency: append-only ndjson envelope stream (deps.ndjson) with
    bidirectional BFS graph queries
- Write-atomicity: tmp file + os.replace (readers never see partial writes).
- Single-process concurrency domain (asyncio locks).
- Trace-bundle data form: directory layout is itself the trace-bundle format
  (ndjson + JSON, no orditect imports required to read).
- Passes full conformance profile (five domains, sink/query paired).