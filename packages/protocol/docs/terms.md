# Orditect Protocol — Consistency Terms

**Version**: 0.1 (draft; subject to revision until the 1.0 freeze)
**Status of this document**: normative. Where a protocol method docstring and
this document disagree, this document wins; the docstring must be fixed.

## Purpose

These terms are the semantic invariants that every storage implementation
(adapter) of orditect-protocol must honor, and that the Orditect frameworks
(core / flow / stream) rely on when interacting with storage through these
contracts.

A term states **what invariant must hold**, never **how to implement it**.
Any term that cannot be traced to an existing discipline in the frameworks'
history is forbidden from entering this document (no speculative terms).

## How to read a term

Each term has four parts:

- **Statement** — the invariant, in one or two sentences.
- **Origin** — the pre-existing framework discipline this term is distilled
  from (the traceability chain).
- **Enforcement** — the contract surface (domain / method / model) where the
  term takes effect.
- **Verification** — the conformance case(s) that verify it, or "review item"
  where automated verification is not possible within a single adapter.

Conformance case numbering is defined in Appendix A.


## Term evolution policy

1. **Append-first.** New semantic needs prefer a NEW term number; the
   semantics of a published term are not modified except via rules 2–3.
2. **Revise.** Clarifications and tightenings are allowed; loosening is
   forbidden (a loosening is a deprecation, rule 3). Every revision keeps
   an in-term Revision note block: old wording, new wording, rationale,
   impact.
3. **Deprecate.** A retired term is marked `superseded-by: Tn`; its number
   stays as a tombstone and is never reused. Conformance-case and data-rule
   numbers follow the same policy.
4. **Change mapping.** Every change is recorded in the CHANGELOG as a
   (term -> semantic change -> impact) triple.

This policy itself is subject to rule 2.
---

## T1 — Lease model (absolute expiry, reader-side filtering)

**Statement.** Every record with a lifecycle expresses it as an absolute
expiry instant (`expire_at`). Readers are responsible for filtering expired
records (lazy cleanup on the read path). Any fallback key-level TTL mechanism
may only ever be extended, never shortened.

**Origin.** taskbase v0.3.2 index ZSET lease-ification (cluster A: member-level
`score = expire_at`, lazy `ZREMRANGEBYSCORE` on read, key-level TTL
increase-only as residue fallback); taskstore backlog §5 (unified lease model:
Redis member-level score and a relational row-level `expire_at` are two
projections of one lease model).

**Enforcement.** `TaskSnapshot.expire_at`; `ResultWriter.save` (carries the
absolute expiry instant); `ResultReader.get` / `SnapshotReader` queries
(filter expired records).

**Verification.** CF-RST-002 (expired read returns None); CF-SNP-004
(expired snapshots invisible to queries).

---

## T2 — Cross-media lease alignment

**Statement.** For one logical entity, its governance-side TTL (Redis), its
snapshot `expire_at`, and its content lifecycle must be aligned to the same
expiry instant. The task's expiry instant is the single source of truth from
which all media projections are computed.

**Origin.** taskstore backlog §5 (unified lease model); taskbase v0.3.2
"index and primary record share the same expiry instant" contract.

**Enforcement.** Term-level obligation on the composing layer (the flow-side
snapshot sink writes `expire_at` aligned with the task lease). The protocol
guarantees the field exists and is comparable (see T7); it cannot guarantee
alignment by itself.

**Verification.** Review item (cross-media alignment spans adapters and
cannot be verified inside one adapter's conformance run).

---

## T3 — Terminal-state irreversibility

**Statement.** Terminal vocabularies are declared and injected by the upper
layer; the protocol embeds none (see T6). Within one execution generation
(one `execution_id`), once a snapshot records a state the caller has declared
terminal, any further state mutation for that generation must be rejected
explicitly with `TerminalStateViolationError`. A new execution generation
(new `execution_id`) is never a mutation of the old one and is always
permitted.

**Origin.** taskbase `task_update.lua` terminal protection (ARGV[6],
unconditional, vocabulary injected by the caller); taskflow R10 vocabulary
wiring; the reopen primitive design (a rerun is a new generation, not a
state regression).

**Enforcement.** `SnapshotWriter.save` (same-generation mutation rule) and
`SnapshotWriter.save_terminal` (explicit terminal declaration);
`errors.TerminalStateViolationError`.

**Verification.** CF-SNP-003 (mutation after terminal state is rejected
explicitly); CF-SNP-005 (new generation with same task_id is accepted).

---

## T4 — Idempotency

**Statement.** Every write interface has an explicit idempotency key.
Re-writing with the same key and an identical payload is a silent success
(deduplication). Re-using the same key with a different payload raises
`IdempotencyConflictError`. `if_not_exists`-style semantics, where offered,
mean the existence check and the write happen in one atomic unit.
Idempotency comparison excludes mechanism clock fields (created_at /
updated_at / registered_at — producer-clock values per T7): two writes
whose business content is identical but whose producer timestamps differ
are the SAME write, never a conflict.

**Origin.** taskbase `task_init.lua` (#14: check-and-write atomicity, exactly
one winner under concurrency); taskflow v0.3.2 call_id dual-habitat key
(hot path and cold path dedup by the same key); taskstore backlog §0
(deterministic ID prefix conventions).

**Enforcement.** `AuditWriter.append` (`event_id`); `SnapshotWriter.save`
(`task_id` + `step` + `execution_id`); `ResultWriter.save` (`stream_id`).

**Verification.** CF-AUD-001 (duplicate append deduplicated); CF-AUD-002
(same key, different payload -> conflict error); CF-SNP-002 (same generation
re-saved idempotently); CF-SNP-006 (concurrent same-key save: exactly one
winner or clean merge, never partial state).

---

## T5 — Pointer discipline

**Statement.** Content above the caller's size threshold must be pointer-ized:
records carry only a `TaskPointer`, never the payload itself. Pointed-to
content is immutable — mutation produces a new pointer, never an in-place
change. Content must be persisted before its pointer is recorded
("content before pointer": a recorded pointer always resolves; orphaned
content without a pointer is reclaimed by reconciliation, which is the
adapter's or the upper layer's responsibility).

**Origin.** taskstore backlog §3 (pointer storage discipline, 10KB
threshold); taskstream `EnrichResult.url` pointer semantics.

**Enforcement.** `ContentWriter.put` (returns `TaskPointer`);
`TaskSnapshot.input_pointer` / `output_pointer`.

**Verification.** CF-CTT-001 (pointer round-trip); CF-CTT-003 (stored
content is immutable under the same pointer). Orphan reconciliation is a
review item.

---

## T6 — Vocabulary neutrality

**Statement.** All status, type, backend, and scope fields in the protocol
are opaque strings. Neither the protocol signatures nor this document may
embed any business vocabulary (no status words, no event-type words, no
backend-specific semantics).

**Origin.** taskbase v0.3.0 state-machine hosting (option B: the framework
presets no vocabulary; the upper layer declares its own).

**Enforcement.** `TaskSnapshot.status`; `AuditEvent.event_type`;
`TaskPointer.backend`; `AuditEvent.scope`.

**Verification.** Review item, assisted by golden schema tests (field
existence is pinned; vocabulary non-embedding is enforced at review).

---

## T7 — Clock discipline

**Statement.** All datetimes in the protocol are timezone-aware UTC. Mixing
naive and aware datetimes in comparisons is a contract violation.

> **Revision 0.1.2** (multi-producer clock reality): the original Statement
> closed with "Values compared across processes are either produced by the
> storage service's own clock or passed as absolute instants — never as
> client-computed durations." That wording implied a single unifiable
> storage clock, which holds for one implementation but not for a
> multi-producer world (bridges on different machines each write with their
> own clock). Revised to: timestamps are **producer-clock** values.
> Comparisons within one producer's sequence are exact; **cross-producer
> comparisons are approximate**, bounded by the producers' clock drift.
> `expire_at` is authoritative **as written by its producer**; a reader
> comparing against its own clock must expect the two clocks' combined
> drift. Rationale: the term's credibility comes from honesty — a promise
> that cannot be enforced across independent producers (a unified clock)
> is weaker than a promise that can (aware-ness + declared drift
> semantics). Impact: no implementation change (existing behavior already
> matches); bridge producers' duty is documented in
> docs/bridge-discipline.md; DR-ALL-001 unaffected (it checks aware-ness,
> not clock sameness).

**Origin.** taskbase B8 (server-side clock for expiry judgement) and #15
(bucket wait computed from the server clock); the Python 3.12
`datetime.utcnow` deprecation fix in this package (all defaults use
`datetime.now(UTC)`).

**Enforcement.** Model default factories (`_utc_now`); the unified mechanism
time-field vocabulary `created_at` / `updated_at` / `expire_at`; `TimeRange`.
(AuditEvent's occurrence field was renamed from `timestamp` to `created_at`
in v0.1.2 to close the cross-domain vocabulary split.)
Bridge producers write with their own timezone-aware UTC clock
(see docs/bridge-discipline.md §4).

**Verification.** Unit and golden tests pin aware-UTC defaults (existing
`tests/unit/test_models.py`, `tests/golden/test_model_schema.py`); adapter
comparative semantics are a review item.

---

## T8 — Explicit capability

**Statement.** An implementation declares its `CapabilitySet`. Invoking an
undeclared half-domain must raise `UnsupportedCapabilityError`. Silent
no-ops and fake successes are forbidden.

**Origin.** taskbase v0.3.1 (DI-mode `close()` raises `InvalidUsageError`:
contract explicitness over silent skipping).

**Enforcement.** `CapabilitySet`; every domain protocol method's docstring
("raises UnsupportedCapabilityError when not supported").

**Verification.** The conformance kit's self-pinning tests
(`tests/unit/test_conformance.py`): undeclared half-domains skip instead of
failing, and a deliberately violating fake turns the suite red.

---

## T9 — Observation non-blocking

**Statement.** The caller wraps sink writes in try/except so that observation
never blocks the business path; the implementation, in turn, must only raise
subclasses of `ContractError` — never bare or ambiguous exception types — so
the caller's handling is total.

**Origin.** taskbase `LimiterHooks` discipline (hook invocation wrapped in
try/except; monitoring never blocks business); taskflow `BudgetAuditSink`
reliability requirement (sink reliability is the implementer's
responsibility: retry + durable buffering).

**Enforcement.** `errors.py` taxonomy; sink method docstrings ("raises only
ContractError subclasses").

**Verification.** Review item, assisted by the fake-implementation exception
surface check in the conformance kit's self-pinning tests
(`tests/unit/test_conformance.py`).

---

## T10 — Concurrency atomicity

**Statement.** Under concurrent writes with the same key, every write
interface must satisfy exactly-one-winner or cleanly-mergeable semantics.
A partially-written state must never be observable.

> **Revision 0.1.2** (concurrency-domain scoping): the original Statement
> did not delimit the concurrency scope, implicitly assuming a single
> process. Revised to: the atomicity guarantee holds **within the
> adapter's declared concurrency domain** — `process` (in-process, e.g.
> memory / local file), `database` (one database's connection domain, e.g.
> PostgreSQL), or `distributed` (cross-node, requiring the backend's own
> distributed-consistency mechanism). Adapters declare their domain via
> `CapabilitySet.concurrency_domain`; CF concurrency cases verify within
> the declared domain. Rationale: precision, not loosening — the guarantee
> was never honestly definable without a scope; a file adapter cannot
> promise cross-process atomicity, and should not pretend to. Impact: no
> behavior change; memory/file declare `process` (the default), future
> relational adapters declare `database`.

**Origin.** taskbase `task_init.lua` (#14: concurrent `if_not_exists`
initialization — exactly one of two writers succeeds); `json_merge.lua`
(atomic read-modify-write).

**Enforcement.** `SnapshotWriter.save`; `AuditWriter.append`;
`ResultWriter.save`; `ContentWriter.put`.
`CapabilitySet.concurrency_domain` (adapter's declared scope).

**Verification.** CF-SNP-006 (concurrent same-key save); CF-RST-003
(concurrent save of the same stream_id).

---

## T11 — Execution identity alignment

**Statement.** `execution_id` is one concept with three projections — the
core hot record, the flow execution, and the protocol snapshot — and all
three must use the same value with the same semantics: each `execute` and
each resume/rerun produces a new `execution_id`. Any divergence between the
projections is a contract violation.

**Origin.** The reopen primitive design (terminal-state rerun conflict
resolution: rerun = new generation, not state regression); the snapshot
version-list requirement (time-travel queries group by
`(task_id, step, execution_id)`).

**Enforcement.** `TaskSnapshot.execution_id` (required field, pinned by
unit tests). Core-side and flow-side projections land in their own
packages' versions (core reopen primitive; flow resume/rerun).

**Verification.** Model-level requirement pinned (`test_execution_id_required`);
cross-framework alignment is a review item at core/flow implementation time.

---

## T12 — Dependency graph as pure-edge facts

**Statement.** The dependency domain stores edges only:
`(child_id, parent_id, is_primary, registered_at)`. Nodes are task_id
references — no node properties are stored in this domain. An edge binds
the task, not an execution generation: reopen (a new execution_id) never
rewrites edges. Cycle detection is not the store's job: the writer records
facts as given; cycle prevention lives with the registrar, cycle discovery
with offline tools. Batch atomicity across edges is not guaranteed;
partial edge writes on failure are compensated by offline rebuild tools.

**Origin.** flow v0.1.1 dep_graph_store injection point and
DependencyGovernor registration semantics; graph-family backend analysis.

**Enforcement.** `DependencyEdge` / `DependencyGraph` models;
`DependencyWriter` / `DependencyReader` protocols.

**Verification.** CF-DEP-001, CF-DEP-004, CF-DEP-005, CF-DEP-006.

---

## Appendix A — Conformance case numbering

Format: `CF-<DOMAIN>-<NNN>`

Domain codes:

| Code | Domain |
|---|---|
| CTT  | Content |
| AUD  | Audit  |
| RST  | Result |
| SNP  | Snapshot |
| ALL  | Cross-cutting (capability, error surface) |
| DEP  | Dependency graph |


Rules:

- Every conformance test's docstring starts with its case id and the term(s)
  it verifies, e.g. `CF-SNP-003 (T3): mutation after terminal is rejected`.
- Case numbers are append-only: never renumber a published case.
- Consumer-profile seeded cases use the `CF-VIEW-NNN` range; they verify
  read-side semantics over pre-seeded data and are cross-referenced from
  the affected terms where applicable (e.g. CF-VIEW-002 under T1).
- Data rules use `DR-<DOMAIN>-<NNN>` numbering under the same append-only /
  tombstone policy; every DR id must be referenced from its term row in
  Appendix B (three-way closure with CF cases).
- CF-SNP-013 (T3): after a terminal save, non-state fields may still merge
  to complete the record; a sparse same-generation save must not erase them.

## Appendix A2 — Conformance profiles

Certification is tiered because implementations come in three shapes:

| Profile | Implementation shape | Hard requirement |
|---|---|---|
| full | storage backend | paired sink/query declarations |
| producer | external-framework bridge | sink half-domains as declared |
| consumer | read-only tool (visualization / diagnostics) | query half-domains as declared |

A profile is a minimum bar, never a ceiling: every declared half-domain is
verified (T8). See docs/conformance.md for the authoritative text. Term
traceability is unaffected: each case keeps its write-half-domain term
mapping; consumer-seeded cases (CF-VIEW-*) verify the read side of the
terms they reference (Appendix B).


## Appendix B — Traceability matrix

| Term | Enforcement surface | Verification |
|---|---|---|
| T1  | snapshot/result models + readers | CF-RST-002, CF-SNP-004, CF-AUD-004, CF-VIEW-002; DR-ALL-002 |
| T2  | composing layer (flow sink)     | review item |
| T3  | SnapshotWriter + errors         | CF-SNP-003, CF-SNP-005; DR-SNP-001, DR-SNP-002 |
| T4  | all writers + errors            | CF-AUD-001, CF-AUD-002, CF-SNP-002, CF-SNP-006, CF-DEP-002, CF-DEP-003; DR-AUD-001 |
| T5  | content writer + snapshot model | CF-CTT-001, CF-CTT-003, CF-CTT-004, CF-CTT-005 (+review); DR-CTT-001 |
| T6  | all opaque-string fields        | review (+golden) + CF-AUD-003, CF-AUD-005, CF-SNP-007, CF-SNP-008, CF-SNP-009, CF-SNP-011, CF-SNP-012, CF-VIEW-004; DR-ALL-003 |
| T7  | all datetime fields             | unit/golden tests (+review); DR-ALL-001 |
| T8  | CapabilitySet + all protocols   | kit self-pinning tests (skip/red) |
| T9  | errors taxonomy + sinks         | review (+kit self-pinning) |
| T10 | all writers                     | CF-SNP-006, CF-RST-003 |
| T11 | TaskSnapshot.execution_id       | unit test, CF-VIEW-001 (+review at core/flow); DR-SNP-003 |
| T12 | dependency models + protocols   | CF-DEP-001, CF-DEP-004, CF-DEP-005, CF-DEP-006, CF-VIEW-003; DR-DEP-001 |

## Appendix C — Mechanism semantics notes

**Aggregate precision (adjudicated v0.1.2).** Cost summation in
`SnapshotReader.aggregate` is IEEE 754 double-precision accumulation with a
committed relative error bound of 1e-9. Aggregated values are for display
and monitoring only — never for reconciliation or billing decisions;
reconcile via the audit domain. (Rationale: switching cost to integer minor
units would churn every model, implementation, and test for no display-level
benefit; if billing-grade needs emerge, evolve via a new field under the
term-evolution policy, not by redefining float semantics.)
(verified by CF-SNP-010).

**Pagination defaults.** `Page(limit=100, offset=0)`; an offset beyond the
result set yields an empty list (never an error).

**Time-range intervals.** `start` is inclusive, `end` is exclusive.