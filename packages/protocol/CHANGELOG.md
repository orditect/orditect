# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.4] - TBD

### Added
- `mechanism.idempotent_payload_equal` + `IDEMPOTENCY_EXCLUDED_FIELDS`:
  T4 idempotency comparison now excludes mechanism clock fields
  (created_at / updated_at / registered_at — producer-clock values per T7).
- New conformance case CF-SNP-013 (T3 second face): a sparse
  same-generation save must not erase previously recorded non-state
  fields.
- Meta-pinning test `TestCaseRegistrationIntegrity`: every CF case must
  be registered under the half-domain its id prefix implies.

### Fixed
- CF-SNP-011/012 were registered under the wrong half-domain
  (`audit_sink` instead of `snapshot_sink`) and were skipped on every
  adapter — sort/group_by whitelist verification never actually ran.
  Moved to `cases_snapshot.py`.
- DR-AUD-001: canonical payload now excludes mechanism clock fields, so
  identical business content with different producer timestamps is a
  legal dedup, not a violation (FLIP).

### Changed
- `domains/snapshot.py` `SnapshotWriter.save` docstring: merge rule
  documented (non-state fields merge to complete the record; status never
  merges).
- `docs/terms.md` T4: idempotency comparison excludes mechanism clock
  fields. Appendix A lists CF-SNP-013.

## [0.1.3] - TBD

### Changed
- Version alignment with ecosystem (additive only, no behavior change).
- Dependency floor check: imports from orditect-protocol predate 0.1.3; floor stays >=0.1,<0.2.

## [0.1.2] - TBD

### Added
- Versioned JSON Schema artifacts (`schemas/`, 6 domains) + drift gate.
- `InvalidQueryError` + mechanism-field whitelists (`mechanism.py`).
- Dependency-graph as fifth domain: DependencyEdge/DependencyGraph models,
  DependencyWriter/DependencyReader protocols, CF-DEP cases, T12 term.
- Conformance profiles (full / producer / consumer) + seeded CF-VIEW cases.
- Data-rule toolkit (`orditect.protocol.rules`): 9 DR rules + run_rules
  reference executor + docs/data-rules.md.
- `CapabilitySet`: dependency_sink/dependency_query half-domains +
  concurrency_domain (T10 Revision).
- Term evolution policy; T7/T10 Revision 0.1.2 (multi-producer clock,
  concurrency-domain scoping).
- docs: wire-format.md, backend-matrix.md, conformance.md, data-rules.md.

### Changed
- **AuditEvent.timestamp renamed to created_at** (unified mechanism
  time-field vocabulary). Serialized key set changed accordingly.
- Conformance runner executes all cases in ONE event loop (adapters with
  loop-bound resources now work).
- Terms document carries Revision notes; traceability is three-way
  (terms <-> CF cases <-> DR rules).

### Compatibility notes
- CapabilitySet: old JSON (8-10 fields) deserializes with new fields
  defaulting (False / "process").
- Sort.field / group_by outside the mechanism whitelist now raises
  InvalidQueryError (previously implementation-defined fallback).

## [0.1.0] - 2026-08-28

First bootstrap of the Orditect storage contract layer. Contracts are
declared stable enough to implement against; the 1.0 freeze requires a
second real backend passing the full conformance kit.

### Added
- B0: Package skeleton (PEP 420 namespace package `orditect.protocol`),
  version single-source via importlib.metadata, namespace coexistence
  pinning tests.
- B1: Error taxonomy (ContractError base + explicit subclasses) and
  CapabilitySet model (8 half-domain flags + protocol compatibility range).
- B2: Data model five-piece set (TaskPointer / TaskSnapshot / AuditEvent /
  Page / Sort / TimeRange) with frozen + compact-serialization disciplines,
  and golden schema snapshot tests pinning serialized key sets.
- B3: Consistency terms v0.1 (`docs/terms.md`): 11 normative terms, each
  with origin traceability and a conformance-case numbering convention.
- B4: Content domain (ContentWriter/ContentReader) and Result domain
  (ResultWriter/ResultReader) narrow protocols; protocol authoring
  convention established (sink/query split, runtime_checkable probing,
  four-element docstring discipline).
- B5: Audit domain (AuditWriter/AuditReader): append-only, event_id as the
  explicit idempotency key, mechanism-field query only.
- B6: Snapshot domain (SnapshotWriter/SnapshotReader): T3/T4/T11 write
  semantics (save vs save_terminal) and seven mechanism-field query
  capabilities mapped to resume / time-travel / DAG / interruption /
  dashboard needs. Tree traversal cycle-safety and depth bounds are
  contract terms.
- B7: Conformance test kit (`orditect.protocol.conformance`) as an
  importable library; capability-gated execution; CF case ids closing the
  traceability loop; self-pinning green/red tests.
- B8: (separate package `orditect-adapter-memory`) in-memory reference
  implementation passing the full conformance suite; establishes the
  per-domain-part adapter structure.
- B9: README with ecosystem position and adapter authoring guide;
  traceability closure check script (scripts/check_traceability.py).