# Orditect Field Notes

**Mental models, contract fine print, and integration pitfalls — a companion
to the official documentation and the `examples/` suite.**

> The official docs describe how each mechanism is *used*. These notes
> describe what the mechanisms *mean*, the contract details that are easy
> to misread, and the traps we hit while running the examples end to end.
>
> Chapter anchors (`Ch.x.x`), rule IDs (`Tx` / `Ax` / `R16`), and data-rule
> codes (`DR-DEP-001`) refer to the official documentation's numbering.
> The official docs remain normative; these notes are field commentary
> verified against the examples at the referenced version.

**Audience:** teams integrating Orditect into business code.

## Table of Contents

- [1. Core Mental Models the Docs Imply but Never State](#1-core-mental-models-the-docs-imply-but-never-state)
  - [1.1 Two governance planes, independently composable](#11-two-governance-planes-independently-composable)
  - [1.2 Vocabulary neutrality is a design principle, not an omission](#12-vocabulary-neutrality-is-a-design-principle-not-an-omission)
  - [1.3 Generations are the universal versioning primitive](#13-generations-are-the-universal-versioning-primitive)
- [2. Key Contracts — the Easily Misread Fine Print](#2-key-contracts--the-easily-misread-fine-print)
- [3. Capabilities the Examples Don't Show (but Fully Support)](#3-capabilities-the-examples-dont-show-but-fully-support)
- [4. Integration Pitfalls Found in Practice](#4-integration-pitfalls-found-in-practice)
- [5. Scope and Contributing](#5-scope-and-contributing)

---

## 1. Core Mental Models the Docs Imply but Never State

### 1.1 Two governance planes, independently composable

| Plane | Components | What it governs | Usable standalone? |
|---|---|---|---|
| **Call plane** (atomic) | `GovernedClient` / `GovernedCallClient` / `GovernedLLMClient` | semaphore, budget, `call_id` idempotency, audit, content pointer-ization | ✅ Yes — embeds into code you already have, no workflow required (proven by `examples/governed-client`) |
| **Task plane** (recursive) | `BaseBackEndTask` + `TaskOrchestrator` | lineage tree, generation snapshots, recovery, HITL, dependency gating | Requires the orchestrator runtime |

What the docs don't say explicitly: the canonical composition is **a task
node's `execute()` wrapping a call-plane client**, producing a
two-dimensional record structure — *node tree × atomic calls*. It is easy
to walk away from the docs believing either that governance only applies
to LLM calls, or that you must build a workflow before you get any
governance. Neither is true.

### 1.2 Vocabulary neutrality is a design principle, not an omission

The framework deliberately refuses to prescribe a dialect:

- `success_words` is declared by the caller (T6).
- `event_type` is an open string. The examples only ever use `"llm_call"`,
  but that is a **convention, not an enum**.
- Resource names are opaque strings with no framework semantics (Ch.3.2);
  the only rule is that registration and acquisition use the same string
  (R16).
- `task_factory` is business semantics — *"mechanism to the framework,
  semantics to the business."*

**Corollary:** recording non-LLM interactions (memory / vector stores /
skills / tools) is **intentionally** not covered by dedicated fields.
Compose it from the mechanisms: your own `event_type` + your own resource
name + content you serialize yourself. Because every example uses only
`llm_call`, readers can easily misread the framework as LLM-only. It is
not.

### 1.3 Generations are the universal versioning primitive

- `reopen_task()` opens a **new execution generation** (new
  `execution_id`) — never a state regression (T3-safe). Old generations
  are retained through the `previous_execution_ids` chain.
- **`execution_id` is the join key across record planes**: snapshot rows,
  the audit `call_id`, and content pointers all aggregate by it.
- Deeper implication: versioning, time travel, and A/B comparison need no
  new concepts — **a generation IS a version**. For workflow-style agents,
  generation history naturally replaces a memory subsystem (a workflow is
  a deterministic input → execute → output DAG; cross-generation memory
  would pollute the purity of version comparison). Within a generation,
  pass data explicitly via `storage.get_task()`.

---

## 2. Key Contracts — the Easily Misread Fine Print

### 2.1 `call_id` dual-habitat idempotency (Ch.11.2) — must embed `execution_id`

`call_id` lives in two habitats at once: it is the quota hot path's
`task_id` **and** the audit cold path's `event_id`. The correct form:

```
{purpose}-{task_id}-{execution_id}
```

- **Same-generation retry** → `already_reserved` dedup: no double charge,
  no double audit.
- **New-generation rerun** → the eid differs, so the call is charged
  normally.

**Trap the docs don't explain:** `examples/mvp` uses `analyze-{task_id}`
(no eid); `examples/real-world` corrects this to include the eid. That
difference is a **bug fix, not a style choice**. Without the eid, a rerun
generation is misidentified as a duplicate retry and **never charged**.

### 2.2 Resource naming and the lineage exemption (Ch.5.3) — the biggest trap in concurrency demos

When a child task uses the **same resource name** as its parent, the
lineage exemption lets the child inherit the parent's semaphore slot —
**no real contention occurs**. To make workers actually queue, use
distinct resource names (e.g. root on `task_execution`, workers on
`worker_exec`). The official examples are all single-resource scenarios,
so this trap only surfaces when you build multi-node concurrency yourself.

### 2.3 `DependencyGovernor` is passive (Ch.8.5) — the caller wiring checklist

It **never creates tasks and never schedules execution**. The caller must
do four things (see `examples/dependency-governance`):

1. Create tasks yourself via `initialize_task` — registration only records
   the relationship.
2. Call `register_dependency(child, parents, primary_parent=...)`.
3. After **every** parent reaches a terminal state, call
   `notify_task_terminal` yourself — the built-in executor never calls it.
   That is the caller contract.
4. Poll `get_ready_tasks()` and submit ready children **yourself**.

**Voting discipline:** a failed parent auto-casts a cancel vote; at
threshold the child is cancelled through the lifecycle (hang prevention).
A vote-cancelled child that never ran has **no snapshot row**, and
`run_rules` reports `DR-DEP-001` — that is a **warning, not a failure**.

### 2.4 Dependency graph ≠ snapshot tree — never conflate the two views

| View | Question it answers | Data source | Caveats |
|---|---|---|---|
| Dependency graph | Structure: who depends on whom | pure-edge facts (T12) | `read_graph(root)` is a **single-root transitive closure** — disconnected components are invisible to each other; enumerate everything with `all_edges()` (the offline scan surface, Ch.7.1) |
| Snapshot tree | State: where the run is | `parent_task_id` lineage | only nodes that actually ran have rows |

### 2.5 Hot-record results are NOT in snapshots — pointer-ize your replay material

- Children read parents' outputs via `storage.get_task()` — the **hot
  path**. Snapshots store only status/generation metadata.
- For version replay, artifact comparison, or generation diffs, you must
  pointer-ize intermediate artifacts through `content_writer` into the
  cold path — otherwise the trace bundle contains nothing to replay.
  Pointer integrity is validated by `run_rules` rule T5.

### 2.6 Budget is a post-charge model

`check()` blocks only when `balance <= 0`: the **last call is allowed to
overspend honestly**, and every subsequent check then blocks. Budget
exhaustion intercepts **before** semaphore acquisition. Pricing is
entirely determined by your `cost_fn` (e.g. real token usage).

### 2.7 Gauge readings are non-atomic approximations (Ch.3.6) — display only

The registry's `usage` / `in_use` readings must **never** drive alerting
or billing decisions. Reconciliation and billing always go through the
audit domain.

### 2.8 The usage-missing stream path (A5) is by design, not an error

Some OpenAI-compatible endpoints (certain Ollama builds) silently return
an empty body when `stream_options` is present → use
`include_usage=False` (developer guide Ch.3.5). Then `cost_fn(None)`
returns 0 and `charge(0)` is a no-op. The audit log shows the call with
`cost_units=0` — **compliant, designed behavior**.

### 2.9 `snapshot_sink` is the master switch for observability

Construct a `TaskOrchestrator` without `snapshot_sink` and the executor
falls back to `NullSink`: the trace bundle stays **completely empty**, and
every UI, validation, and recovery read sees no data — **with no error
raised**.

### 2.10 Streaming is an additional output plane, not a governance replacement

`StreamRunner` runs **inside** a governed node (see `examples/stream`).
The aggregated content must still be returned as the task result so the
snapshot/result/recovery chain stays intact. The enrich pipeline
(marker → placeholder → resolved → manifest) is the native mechanism for
rich blocks (images etc.); the manifest lands in the trace bundle.

### 2.11 The action queue is asynchronous with dual receipts (Ch.10.3)

HITL actions: the sink returns an **acceptance receipt**; the **execution
receipt** is polled via `get_receipt`. Action effects (node → `cancelled`,
etc.) become visible through snapshot polling. The run-scoped queue is
destroyed when the run ends — receipts do not survive across runs.

---

## 3. Capabilities the Examples Don't Show (but Fully Support)

### 3.1 Governing non-LLM interactions

`GovernedCallClient`'s handler is any async callable. To wrap tools,
vector stores, or skills:

- choose a custom `event_type` (e.g. `tool_call` / `vector_query`),
- choose a custom `cost_fn` (price per call or per duration),
- pointer-ize both input parameters and output results.

The governance five-piece — semaphore / budget / idempotency / audit /
pointer-ization — applies unchanged. This is the intended path to **full
interaction traceability inside an agent**.

### 3.2 Dynamic DAGs: structure decided at runtime

Submitting children inside `execute()` means the DAG's shape can be
decided **at runtime** — e.g. an LLM planner decomposes a problem into N
subtasks, then dynamically submits N workers and dynamically writes
dependency edges. Different generations may produce DAGs with different
node counts. The examples are all static, but the mechanism imposes no
such restriction — this is the path to planning capability and agent
autonomy.

### 3.3 Three levels of time travel

| Granularity | Mechanism | Semantics |
|---|---|---|
| Node | `retry_scope(node)` | a single node opens a new generation; every other node stays frozen at its point in time |
| Subtree | `resume_tree(root)` | succeeded nodes are **reused** (decided by `reuse_terminal_words`; not re-executed, not charged); failed/cancelled nodes reopen new generations → a mixed timeline |
| Global | whole-tree reopen | all nodes open new generations |

Reuse semantics depend on the `call_id`-with-eid discipline (see 2.1):
reuse = skip execution; rerun = new eid, charged normally.

### 3.4 Version provenance and diffs

Filter audit events by eid + pull the content pointers → a complete
single-generation profile (status / spend / artifacts / tool-call
sequence). Two eids side by side = a version diff. Same input across
different generations lets you compare an agent's tool-call path
divergence — decision-level observability.

### 3.5 Positioning: governance kernel vs. observability tooling

Orditect is a **governance kernel** (pre-execution blocking + in-flight
control + writable recovery), not a Langfuse-style observability layer
(post-hoc, read-only). The essential differences: semaphore-based hard
concurrency control, real-time budget blocking, `call_id` idempotency
against double-charging, HITL write operations, and snapshot-based
recovery execution. Do not integrate it as a tracing backend.

---

## 4. Integration Pitfalls Found in Practice

1. **Retryable parents: every internal child submission must pass
   `if_not_exists=True`.** Otherwise a parent rerun re-initializes its
   children — new generations, double charging, duplicated output. Guard
   leaf nodes and fan-in nodes symmetrically.
2. **`CancellationToken` sync/async mismatch across packages.**
   `orditect-stream`'s `is_cancelled()` is synchronous; flow-side governed
   clients `await` it. Passing the raw token raises `TypeError` — wrap it
   in an async adapter (see `_AsyncCancelToken` in
   `examples/stream/tasks.py`). An official adapter utility would be
   preferable to every team rediscovering this independently.
3. **`task_factory` cannot see the original run's constructor arguments.**
   `RecoveryService` rebuilds tasks from `task_id` alone; original kwargs
   are captured via closure, and circular dependencies (e.g. the
   orchestrator) need late binding. Distinguishing "first execution" from
   "old-generation rerun" behavior (e.g. whether to re-enter a delay
   window) requires a business-side run-marker convention.
4. **Clean hot-path residue of deterministic task_ids before a run.**
   Reusing fixed task_ids together with `if_not_exists=True` lets
   leftovers from an interrupted run short-circuit initialization,
   silently skipping the executor lifecycle — no snapshots, no audit.
5. **Verify stream-endpoint usage compatibility (see 2.8).** Test the
   `stream_options` behavior of your target endpoint before going live.

---

## 5. Scope and Contributing

These notes describe behavior verified against `examples/` at the
referenced version. If anything here contradicts the official
documentation or observed behavior, please open an issue — treat the
official docs as normative and these notes as field commentary.
Corrections and additional traps from real integrations are welcome via
pull request.