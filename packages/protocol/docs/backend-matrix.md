# Backend-family semantics matrix

Which domains each backend family can meaningfully implement, and at what
scale. "Required" = the domain is the family's home turf; "optional" =
implementable where it makes sense; "n/a" = no meaningful implementation.

| Family | content | audit | result | snapshot | dependency |
|---|---|---|---|---|---|
| Relational (PG / SQLite) | optional | required | required | required | required |
| Document (localfile) | required | required | required | required | required |
| KV (redis cold-side) | optional | optional | required (warm) | optional | optional |
| Object store (MinIO / S3) | required | n/a | optional | n/a | n/a |
| Vector (Milvus) | required * | n/a | n/a | n/a | n/a |
| Graph (Nebula-class) | n/a | n/a | n/a | projection ** | required *** |

Notes:

- \* Vector family: similarity retrieval is permanently outside the
  protocol (private adapter interfaces); the protocol covers content
  put/get/delete/metadata only.
- \*\* Graph family consumes the snapshot domain only as an *enriched
  projection* (derived, discardable, rebuildable) — never as authoritative
  storage. Node properties stay in the snapshot domain (pure-edge, T12).
- \*\*\* Graph family has no decisive advantage over the relational family
  below ~10k nodes for dependency-neighbourhood reads; do not adopt it for
  its own sake.

**KV-family caveat (redis cold-side).** A redis cold-side adapter (protocol
domains on Redis) and the governance hot path's Redis are TWO different
things: the former is replaceable protocol implementation, the latter is
not abstracted and not replaceable. They may co-exist in one instance under
different key prefixes, but they are always two semantic layers. The redis
cold-side adapter is a demo/learning-oriented KV reference, not a long-term
archive.