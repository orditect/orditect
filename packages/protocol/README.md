# orditect-protocol

**Storage interaction contracts for the Orditect ecosystem**

[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)

`orditect-protocol` is the **connection contract between the Orditect
frameworks and storage backends**. It defines *what is stored and how it is
retrieved* — never *how a backend implements it*.

It is the layer that lets `orditect-core` / `orditect-flow` /
`orditect-stream` store content, audit events, results, and execution
snapshots against one contract, and lets any backend (PostgreSQL, MinIO,
Milvus, memory, local JSON, ...) plug in by implementing the same contract.

## Position in the Orditect ecosystem
```
orditect-core / orditect-flow / orditect-stream
        |  store & query via this contract
        v
orditect-protocol        <-- THIS PACKAGE (contracts only, zero implementation)
        |  implemented by
        v
storage adapters: orditect-adapter-memory (open, reference) /
                  PostgreSQL / MinIO / Milvus / ... (commercial)
```

**The governance hot path is deliberately NOT here.** Semaphores, token
buckets, quotas, task state, and the lineage hot index live in
`orditect-core`, pinned to Redis (millisecond-sensitive, Lua-atomic). This
package governs only the content side: where content, snapshots, audit, and
results go.

## Boundary discipline (red lines)

1. **Zero implementation** — contracts and models only; no storage logic.
2. **Zero Redis dialect** — no ZSET / Lua / TTL keywords in any signature or
   docstring; terms speak of leases, terminal states, idempotency keys.
3. **Zero business semantics** — no WHERE-style business query DSL; only
   mechanism fields (time range, pagination, status-as-opaque-string).

## The four domains

Each domain is a narrow sink/query protocol pair. An implementation declares
the half-domains it supports via `CapabilitySet`; the rest must raise
`UnsupportedCapabilityError` (never silently no-op, term T8).

| Domain | Sink | Query | Purpose |
|---|---|---|---|
| Content  | `ContentWriter`  | `ContentReader`  | Pointer-ized content bodies (all modalities) |
| Audit    | `AuditWriter`    | `AuditReader`    | Append-only idempotent event log |
| Result   | `ResultWriter`   | `ResultReader`   | Stream manifests with true TTL |
| Snapshot | `SnapshotWriter` | `SnapshotReader` | Execution snapshots + tree/version queries |
| Dependency | `DependencyWriter` | `DependencyReader` | Pure-edge dependency facts + graph queries (T12) |
## Consistency terms

Eleven normative terms (lease model, terminal irreversibility, idempotency,
pointer discipline, vocabulary neutrality, clock discipline, explicit
capability, observation non-blocking, concurrency atomicity, execution
identity alignment, cross-media alignment) are frozen in
[`docs/terms.md`](docs/terms.md). Every protocol method's docstring references
the terms it enforces.

## Implementing against the contract

Three implementation shapes, three compliance tiers:

### Storage backend (full)

Implement the half-domains you support, structured as per-domain parts
(see `orditect-adapter-memory` for the reference structure), declare them
in a `CapabilitySet` — sink and query **in pairs** — and certify:

```python
from orditect.protocol.conformance import run_conformance

def test_conformance():
    report = run_conformance(my_store.snapshot, profile="full")
    assert report.eligibility_error is None
    assert report.failed == 0, report.summary()
```


### Bridge / external-framework producer (producer)

A bridge only needs to **write** correctly: implement the sink half-domains
(`snapshot_sink` + `audit_sink` is a typical start, plus `dependency_sink`
when wiring dependency governance) and certify under the producer tier:
```python
report = run_conformance(my_bridge, profile="producer")
assert report.failed == 0, report.summary()
```

Business vocabulary of the external framework is translated to opaque
strings at the bridge edge — never flowing back into any framework package
(see docs/bridge-discipline.md).

### Read-only consumer (consumer)

Visualization / diagnostics tools implement the query half-domains and,
for deep read verification, the `seed()` hook:
```python
report = run_conformance(my_viewer, profile="consumer")
assert report.failed == 0, report.summary()
```
**Passing the kit under the appropriate tier is the only compatibility
certification.** Undeclared half-domains skip, never fail (T8).



## Versioning

Independent version line. Terms changes are minor/major bumps with a
CHANGELOG entry. The 1.0 freeze requires a second real backend (a production
relational adapter) passing the full conformance kit.

## License

Apache-2.0
```