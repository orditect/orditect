# Orditect Examples

Runnable demonstrations of the governance kernel, from a single governed
call to a production-grade streaming workflow. Each example is
self-contained: a `run_demo.py` entrypoint, its own `requirements.txt`,
and a `bootstrap.py` that puts the in-repo packages on `sys.path` —
**no package installation required**.

> Read these alongside the [Field Notes](../docs/NOTES.md), which
> explain the mental models (two governance planes, vocabulary neutrality,
> generations-as-versions) and the contract fine print behind what you see
> here. Sections below reference it as "Field Notes §x.y".

## Quick start

Requires Python 3.12+.

```bash
pip install -r examples/mvp/requirements.txt
python examples/mvp/run_demo.py
```

Every demo prints its stages and ends with `DEMO OK`. Run all commands
from the repository root.

## Learning path

The examples build on each other in a deliberate order:

| # | Example | Governance plane | Infrastructure | What it proves |
|---|---------|------------------|----------------|----------------|
| 1 | [governed-client](#1-governed-client--governance-without-a-workflow) | Call | none | governance embeds into code you already have |
| 2 | [mvp](#2-mvp--the-full-governance-loop-zero-infrastructure) | Task | none | the full loop: workflow → visualize → HITL → validate |
| 3 | [dependency-governance](#3-dependency-governance--multi-parent-fan-in) | Task | none | multi-parent fan-in with the passive DependencyGovernor |
| 4 | [real-world](#4-real-world--the-production-hot-path) | Task | Redis + LLM endpoint | hot/cold replaceability: same business code, production infra |
| 5 | [stream](#5-stream--the-sse-output-plane) | Task + Stream | Redis + LLM endpoint | governed nodes can stream live output to end users |

Examples 1–3 run with **zero infrastructure**: in-memory hot-path doubles
plus a mock LLM served over `httpx.MockTransport` (no API key needed).
Examples 4–5 swap the doubles for the production Redis trio and a real
OpenAI-compatible endpoint — with **no business-code change**
(Field Notes §1.1).

---

## 1. governed-client — governance without a workflow

```bash
python examples/governed-client/run_demo.py
```

No workflow, no orchestrator. Wraps a plain pre-existing async callable
in governance, in four stages:

1. `GovernedClient` — semaphore + budget + `call_id` idempotency
   (a retry with the same `call_id` is charged exactly once).
2. `GovernedCallClient` — adds audit events and content pointer-ization.
3. Streaming — the semaphore is held for the stream's whole lifetime,
   audit is written at stream close, and a usage-missing stream is priced
   at 0 units (Field Notes §2.8).
4. Budget exhaustion — blocked **before** semaphore acquire; the last
   call before exhaustion is allowed to overspend honestly
   (Field Notes §2.6).

**Start here if** you have existing code calling an LLM (or any tool) and
want cost/concurrency control today. See Field Notes §1.1 on why the
call plane stands alone, and §3.1 on governing non-LLM callables.

## 2. mvp — the full governance loop, zero infrastructure

```bash
python examples/mvp/run_demo.py
```

A recursive pipeline (`collect → analyze → report`) where the root task
submits its children inside `execute()` and `parent_task_id` is injected
automatically. The `report` node fails on its first generation on
purpose. Then:

- **Visualize** — lineage tree, execution generations (time travel),
  dependency graph, audit log, status aggregates. All views read the
  trace bundle via `TraceBundleReader`, never framework internals.
- **HITL retry** — `retry_scope("report")` through the action queue
  reopens a **new execution generation** (Field Notes §1.3).
- **HITL pause/resume** — a slow node is cooperatively cancelled, then
  `resume_tree` recovers it (Field Notes §3.3).
- **Validate** — `run_rules` over the trace bundle, zero violations.

**Fine print to notice while running:**

- The action queue is asynchronous with **dual receipts** (acceptance vs.
  execution) — Field Notes §2.11.
- Without `snapshot_sink` the trace bundle would be silently empty —
  Field Notes §2.9.
- This example deliberately uses `call_id = analyze-{task_id}` **without
  an execution_id**. That is the minimal form; `real-world` corrects it.
  The difference is a bug fix, not a style choice — Field Notes §2.1.

## 3. dependency-governance — multi-parent fan-in

```bash
python examples/dependency-governance/run_demo.py
```

`C` runs only after `A` **and** `B` finish — the case a linear pipeline
cannot express. The `DependencyGovernor` is **passive**: it never creates
tasks and never schedules execution, so the demo drives the full
caller-side wiring contract (Field Notes §2.3):

1. `initialize_task` each node yourself;
2. `register_dependency(child, parents, primary_parent=...)`;
3. call `notify_task_terminal` after **every** parent reaches a terminal
   state — the built-in executor never calls it for you;
4. poll `get_ready_tasks()` and submit ready children yourself.

It also demonstrates the **voting discipline** (both parents fail →
threshold reached → child cancelled via lifecycle, preventing a permanent
hang), and the **two views that must not be conflated**: the dependency
graph (structure) vs. the snapshot tree (state) — Field Notes §2.4. A
vote-cancelled child that never ran has no snapshot row; `run_rules`
reports exactly one `DR-DEP-001` **warning**, not a failure.

## 4. real-world — the production hot path

```bash
cp examples/real-world/.env.example examples/real-world/.env  # edit values
python examples/real-world/run_demo.py
```

**Identical business code to `mvp`** — only the hot-path doubles and the
LLM endpoint are swapped for their production counterparts:

| Concern | Examples 1–3 | Here |
|---|---|---|
| Task store | `InMemoryTaskStorage` | `TaskRedisDB` |
| Semaphore | `InMemoryGovernor` | `AsyncLeaseSemaphore` via `LimiterRegistry` |
| Quota | `InMemoryQuota` | `AdmissionQuotaRedisDB` |
| LLM | `httpx.MockTransport` | your OpenAI-compatible endpoint |

This is the living proof of hot/cold replaceability. Note the corrected
`call_id = {purpose}-{task_id}-{execution_id}` discipline (Field Notes
§2.1), and remember that registry gauge readings are display-only —
billing and reconciliation go through the audit domain (§2.7).

> **Rerunning:** the demo uses deterministic task IDs. Leftover hot-path
> state in Redis from an interrupted run can short-circuit initialization
> (Field Notes §4.4). Use a dedicated Redis DB for demos, or flush it
> between runs.

## 5. stream — the SSE output plane

```bash
cp examples/stream/.env.example examples/stream/.env  # edit values
python examples/stream/run_demo.py
```

Same Redis hot path and real LLM as `real-world`, but the `analyze` node
runs a `StreamRunner` **inside** its governed `execute()`: LLM output
becomes a live protocol event sequence (`stream.delta` / `enrich.*` /
`stage.end` / `stream.manifest`), printed as readable SSE frames. The
aggregated content is still returned as the task result, so the
snapshot/result/recovery chain stays intact (Field Notes §2.10).

Also worth studying in `tasks.py`:

- `_AsyncCancelToken` — an adapter for the sync/async `CancellationToken`
  mismatch across packages (Field Notes §4.2).
- `include_usage=False` — the A5 compatibility path for endpoints that
  silently return an empty body when `stream_options` is present
  (Field Notes §2.8).
- Ctrl+C cancels the task tree gracefully; partial content is preserved
  in the trace bundle.

---

## Common conventions

**`bootstrap.py`.** Every example injects `packages/*/src` into
`sys.path`, mirroring the repository's test-suite pattern. Import it
**before** any `orditect` import; run demos from the repository root.

**The trace bundle.** Each run recreates `examples/<name>/demo_data/`
and writes the cold-path bundle to `demo_data/trace/`:

| File | Domain |
|---|---|
| `snapshots.ndjson` | lifecycle snapshots (one row per generation transition) |
| `audit.ndjson` | append-only, idempotent event log (billing/reconciliation source of truth) |
| `deps.ndjson` | dependency edges (pure-edge facts, T12) |

**Hot-path doubles (`infra.py`).** The in-memory implementations satisfy
the same duck-typed protocols as the production Redis trio. Swapping one
for the other requires no business-code change — that is the point of
the hot/cold separation contract.

## Feature matrix

| Capability | governed-client | mvp | dependency-governance | real-world | stream |
|---|:-:|:-:|:-:|:-:|:-:|
| Semaphore (concurrency) | ✓ | ✓ | ✓ | ✓ | ✓ |
| Budget ledger | ✓ | ✓ | ✓ *(opened, unused)* | ✓ | ✓ |
| `call_id` idempotency | ✓ | ✓ *¹* | – | ✓ | ✓ |
| Audit trail | ✓ | ✓ | ✓ *(governor events)* | ✓ | ✓ |
| Content pointer-ization | ✓ | ✓ | – | ✓ | ✓ |
| Lineage tree & generations | – | ✓ | ✓ | ✓ | ✓ |
| HITL actions (retry/pause/resume) | – | ✓ | – | ✓ | ✓ *(retry)* |
| Dependency edges (T12) | – | ✓ | ✓ | ✓ | ✓ |
| DependencyGovernor (fan-in + voting) | – | – | ✓ | – | – |
| Streaming output plane | ✓ *(call-level)* | – | – | – | ✓ *(node-level)* |
| Redis hot path | – | – | – | ✓ | ✓ |
| Real LLM endpoint | – | – | – | ✓ | ✓ |

*¹ Uses `call_id` without `execution_id`; see Field Notes §2.1 for why
`real-world` and `stream` include it.*

## Choosing where to start

- **"I want cost/concurrency control over calls I already make"** →
  `governed-client`
- **"I want to see the whole system end to end"** → `mvp`
- **"My workflow needs joins / fan-in"** → `dependency-governance`
- **"I'm evaluating for production"** → `real-world`, then `stream`

## Configuration (real-world / stream)

Both read `examples/<name>/.env` (see `.env.example`):

| Variable | Default | Purpose |
|---|---|---|
| `LLM_BASE_URL` | `http://localhost:11434/v1` | any OpenAI-compatible endpoint (Ollama, vLLM, OpenAI) |
| `LLM_API_KEY` | `ollama` | API key |
| `LLM_MODEL` | `qwen2.5:7b` | model name |
| `REDIS_URL` | `redis://localhost:6379/0` | hot-path Redis |
| `LLM_SEM_LIMIT` | `30` | concurrency limit for the `llm` resource |
| `BUDGET_MAX_UNITS` | `100000` | budget ceiling for the run |

With Ollama: `ollama serve` and `ollama pull qwen2.5:7b`, then run the
demo. Before going live against a custom endpoint, verify its
`stream_options` behavior (Field Notes §2.8).