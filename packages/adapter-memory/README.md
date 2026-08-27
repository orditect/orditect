# orditect-adapter-memory

In-memory reference implementation of the
[`orditect-protocol`](../protocol) storage contracts.

## Purpose

- Reference for adapter authors: demonstrates the recommended structure
  (per-domain parts composed under one store facade).
- Development/test backend for the Orditect frameworks.

## Structure

`MemoryStore` composes five per-domain parts, each implementing one
protocol pair:

    store = MemoryStore()
    store.content     # ContentWriter + ContentReader
    store.audit       # AuditWriter + AuditReader
    store.result      # ResultWriter + ResultReader
    store.snapshot    # SnapshotWriter + SnapshotReader
    store.dependency  # DependencyWriter + DependencyReader

Frameworks consume the part they need; each part exposes its own
`capabilities` property. This per-domain-part structure is the recommended
shape for production adapters (avoids method-name collisions across domains).
## Scope

Single-process, non-durable. No cross-process consistency, no persistence.
Do not use in production.