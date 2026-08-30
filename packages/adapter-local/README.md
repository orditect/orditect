# orditect-adapter-local

Local-file reference implementation of the
[`orditect-protocol`](../protocol) storage contracts (document-family).

## Purpose

- Document-family representative for the protocol 1.0 freeze criterion.
- Zero-infrastructure backend: the full recovery/governance plane runs
  against plain files — no Redis, no database, no services.
- First producer of the **trace bundle** data form: a directory layout of
  ndjson envelope rows + JSON payloads, readable by any consumer without
  importing orditect.

## Layout (trace bundle form)

    <root>/
      snapshots.ndjson      # op envelope rows: {"v","op","ts","data"}
      audit.ndjson          # op "append"
      deps.ndjson           # op "edge_write"
      results/<stream_id>.json
      content/sha256/<aa>/<digest>          # content-addressed blobs
      content/sha256/<aa>/<digest>.meta.json

Writers are single-process and write-atomic (tmp file + rename). Readers
fold the ndjson streams into their query views on access.

## Capability declaration

`concurrency_domain = "process"` (T10 Revision 0.1.2): the atomicity
guarantee holds within one process (guarded by an asyncio lock). A second
process may read/write the same directory, but cross-process atomicity is
NOT declared.

## Scale boundary

Designed for ~10k-row scale per stream file (read-path folding is O(n)).
This is the document-family *reference* — not a production-scale store.

## Structure

    store = LocalFileStore("/path/to/dir")
    store.content     # ContentWriter + ContentReader
    store.audit       # AuditWriter + AuditReader
    store.result      # ResultWriter + ResultReader
    store.snapshot    # SnapshotWriter + SnapshotReader
    store.dependency  # DependencyWriter + DependencyReader

Each part is independently usable and exposes its own `capabilities`
property, mirroring the per-domain-part structure of the memory adapter.

## Durability & append semantics

- `atomic_write_*` (content blobs, result manifests) uses tmp-file + rename:
  readers never observe a partially written file.
- ndjson appends (snapshots / audit / deps) are **best-effort**: a crash
  mid-append may lose the tail row. Torn tail rows are skipped silently on
  read (by design, matching append-only log semantics).
- The adapter does **not** fsync. For crash durability, use the PostgreSQL
  adapter. This is a development / small-scale reference backend.
- Read paths fold streams on access (O(n) per query); write paths that
  dedup or guard scan the stream first (O(n) per write). Sized for ~10k
  rows per stream file, not production scale.