# Data rules (DR): machine-checkable invariants over serialized data

**Status**: normative. This document is the authoritative text of every
data rule; the code under `orditect/protocol/rules/` is its reference
implementation.

Data rules verify **data products** (serialized records, envelopes,
bundles), complementing the conformance suite which verifies **adapter
behavior** (API level). A producer in any language can self-certify its
output by running these rules over it — that is what makes the protocol a
format, not a library.

## 1. Numbering

`DR-<DOMAIN>-<NNN>`; domain codes mirror the CF codes:
CTT / AUD / RST / SNP / DEP / ALL. Numbers are append-only, never reused,
and retired numbers stay as tombstones (same policy as the term-evolution
policy).

## 2. Level criterion

> **violation** = an invariant that holds under EVERY legal use is broken.
> **warning** = the state can be legal (e.g. T1 expiry makes dangling
> references legitimate) but deserves a human look.

Consequence: all cross-domain reference checks default to warning. The only
violation-level reference rule is DR-CTT-001, because T5 promises "a
recorded pointer always resolves" unconditionally — with one declared
exemption channel (§4).

## 3. Meta-rules (every rule implementation must satisfy)

1. Rules are pure functions: `Iterable[dict] -> list[Finding]`. No IO.
2. No state shared across rules; cross-row accumulation is private to one
   rule invocation (input is a single serial stream).
3. Input is the **envelope** stream: `{"v": 1, "op": ..., "ts": ..., "data":
   {model payload}}`. The `op` key is OPTIONAL: rules that depend on op
   degrade explicitly when it is absent (see each rule's spec) and mark
   their findings `"degraded": true`.
4. Every finding carries rule id, level, location, message, term reference.

## 4. Exemption channel: dangling_pointers

Legal deletion (content.delete) legitimately orphans a pointer. Producers
declare this by emitting a metadata row in the stream:

```json
{"meta": "dangling_pointers", "keys": ["sha256/ab/cdef..."]}
```

Registered keys are exempt from DR-CTT-001. This makes compliant deletion a
data-level declarable fact instead of implicit knowledge.

## 5. Rule specs

| Rule | Level | Term | Spec |
|---|---|---|---|
| DR-SNP-001 | violation | T3 | With op: after `save_terminal` for key (task_id, step, execution_id), any later row for the same key with a different status → violation. Without op: any status drift for the same key → violation marked degraded. |
| DR-SNP-002 | violation | T3 | After `save_terminal` for a key, ANY later `save`/`save_terminal` for the same key whose status differs from the terminal one → violation (op-sequence legality itself). Requires op; skipped (not degraded) when op is absent. |
| DR-SNP-003 | violation | T11 | Any snapshot row with empty/missing execution_id → violation. |
| DR-AUD-001 | violation | T4 | Same event_id with different payload → violation. Identical payload repeats are legal dedup, not reported. |
| DR-CTT-001 | violation | T5 | A snapshot row's input_pointer/output_pointer key absent from the content rows AND not registered in dangling_pointers → violation. |
| DR-ALL-001 | violation | T7 | Any datetime field value lacking an explicit offset → violation. "Z" and "+HH:MM"/"-HH:MM" are both accepted. |
| DR-ALL-002 | warning | T1 | audit.task_id with no snapshot row for that task (may be T1-expired) → warning. |
| DR-DEP-001 | warning | T12 | an edge whose child_id or parent_id has no snapshot row (same caveat) → warning. |
| DR-ALL-003 | warning | det-ID | manifest placeholder task_ref suffix ≠ placeholder_id (`tf:enrich-{pid}` convention) → warning. |

## 6. Lineage

fixtures (M3, conformance consumer tier) → envelope/op semantics (M1
wire-format §7) → these rules → future YAML test vectors. The dict shapes
are one lineage; do not let them drift apart.

## 7. Verification discipline

- Passing all violation rules does NOT mean full compliance: T2
  (cross-media alignment) and T9 (observation non-blocking) are not
  checkable at data level and remain review items.
- The validator NEVER judges readiness, NEVER implies scheduling, and
  NEVER interprets business vocabulary.
- API level (conformance) and data level (rules) are complementary;
  neither replaces the other.
