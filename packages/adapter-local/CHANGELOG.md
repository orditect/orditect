# orditect-adapter-local Changelog

## [0.1.6] - TBD

### Changed
- Version alignment with ecosystem (additive only, no behavior change).
- Dependency floor raised: orditect-protocol>=0.1.6.

## [0.1.5] - TBD

### Fixed
- `_fold` rewritten on `mechanism.fold_snapshot_rows` — status no longer
  regresses on empty-status sparse saves, and `created_at` no longer
  drifts on merge (both were silent record corruption).
- `save()` terminal guard: empty status (absence of intent) no longer
  raises TerminalStateViolationError.
- Sorting by `expire_at` no longer misorders via the string "None" —
  no-expiry sorts as infinitely far.
- result part: `stream_id` is validated as a safe path segment (no
  traversal).

### Changed
- README: append is best-effort (torn tail rows skipped by design); no
  fsync; scale boundary tightened (O(n) read-fold and write-scan).

### Tests
- New CF-SNP-014/015 mirrors in `test_local_semantics.py`.

## [0.1.4] - TBD

### Fixed
- T4 idempotency: `save_terminal` and `append` now compare business
  content excluding mechanism clock fields via
  `mechanism.idempotent_payload_equal` — a reconstructed retry with a new
  producer timestamp is a silent dedup, not a conflict. (The existing
  `test_save_terminal_identical_resave_dedups` now actually passes.)
- T3 second face: `_fold` merges non-state fields to complete the record
  (status taken from the latest row; previously it froze the first row's
  status, causing terminal-state false rejections on sparse re-saves).

### Tests
- New `test_idempotency_semantics.py`: reconstructed-retry dedup pins.
- New `test_sparse_resave_preserves_cost`: CF-SNP-013 mirror.

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