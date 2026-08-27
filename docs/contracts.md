# Orditect Contracts (meta-terms)

> This document constrains **the framework's architecture and the
> composition layer**. Adapter behavior is constrained by
> `packages/protocol/docs/terms.md` — the two documents have different
> audiences and deliberately do not repeat each other: this document states
> the contracts and maps them onto the terms, gates, and docs that carry
> them.

## Contract 1 — Orchestration independence (mechanism, not orchestration)

**Statement.** The framework embeds no orchestration semantics: no built-in
DAG scheduling, no automatic inference of dependency-driven reruns, no
task-state progression unless externally invoked.

Precision (v0.1.2 clarification): "no orchestration" does NOT mean "no
driving code". Watchdogs, heartbeats, `WorkflowExecutor`, and
`RecoveryService` are legitimate MECHANISM components. The criterion is:
a component is orchestration if it **decides what runs next** from
dependency/business semantics it owns, or **advances task state without an
external call**. A mechanism component only maintains an already-acquired
lease, executes an explicitly submitted unit of work, or answers a
passive query.

**Constrains**: orditect-core / flow / stream framework code; the
composition layer (callers).

**Carried by**: T6 / T8 at the protocol layer (no active verbs on the
contract surface — `scripts/gates/check_api_surface.py`);
`packages/core/docs/lua_contract.md` at the hot path.

**Verified by**: `check_api_surface.py` (gate); review (mechanism-vs-
orchestration criterion applied to new components).

## Contract 2 — Hot/cold separation

**Statement.** Redis holds the transient state of in-flight tasks;
snapshots, audits, results, content bodies, and the dependency graph live
in external stores behind the orditect-protocol contracts.

Precision (v0.1.2 clarification): what is separated is the **data model
and the access path**, NOT the deployment topology. The memory adapter
(hot and cold in one process) is legal; a single PostgreSQL serving both
cold sides is legal; a redis cold-side adapter and the governance hot
path's Redis are TWO different things that may co-exist in one instance
under different key prefixes but are always two semantic layers. The
governance hot path is pinned to Redis + Lua and is never abstracted.

**Constrains**: framework code; adapter authors (cold side); deployers
(hot-side Redis ownership).

**Carried by**: T1 / T2 / T5; `packages/protocol/docs/backend-matrix.md`
(six-family semantics matrix).

**Verified by**: review + `check_business_neutrality.py` (vocabulary side
of "the hot path is not protocolized").

## Contract 3 — Deployment neutrality

**Statement.** The governance kernel runs entirely inside the customer's
own environment (VPC, on-prem, a single container). The hot path has zero
external network dependencies. Any SaaS control plane is a convenience
layer for cold-data aggregation only — never a prerequisite for governance
capability.

**Constrains**: framework code; deployers.

**Deployment guidance (SHOULD-level, intentionally NOT gated)**:
- Hot-path latency budget < 10 ms: co-locate the governance Redis in the
  same host / availability zone.
- The governance Redis lives inside the customer VPC.
- Cold-side topology is free (any protocol-conformant backend).

These are SHOULDs, not MUSTs: a cross-AZ highly-available Redis (1–2 ms)
is a legitimate deployment; no gate may reject it.

**Carried by**: T7 (server-side clock); DD-013 (hot-path zero external
dependency).

**Verified by**: `check_import_boundary.py` (no cloud SDK or business
package may enter the framework import graph); review.

## Appendix — term <-> contract mapping

| Contract | Carrying terms | Carrying gates / docs |
|---|---|---|
| 1 orchestration independence | T6, T8 | check_api_surface.py; lua_contract.md |
| 2 hot/cold separation | T1, T2, T5 | backend-matrix.md; review |
| 3 deployment neutrality | T7 | check_import_boundary.py; DD-013 |

Terms T3/T4/T9/T10/T11/T12 are adapter-behavior invariants (see terms.md);
they are not contract-specific but uphold all three contracts at the data
plane.