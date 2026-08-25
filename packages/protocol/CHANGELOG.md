# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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