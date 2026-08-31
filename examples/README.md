# Orditect Developer Guide

Build governed AI workflows **without reading the framework source**. This
guide covers everything you can build with the open Orditect packages:
Redis hot path, local-file cold path, adapter-ui visualization, DAG
observability, SSE output plane, HITL intervention, budget settlement, and
the recovery plane.

**What this guide is NOT**: adapter development (PostgreSQL / MinIO /
Milvus), bridge implementations (LangChain / AutoGen / etc.), or a
production frontend (React/Vue is your own product layer). Those are
linked at the end where relevant.

**Prerequisites**: Python 3.12+. Read chapters in order — each builds on
the previous. Every capability has a runnable example under `examples/`.

---

## Chapter 1 — Mental model (read this first)

Orditect governs **AI workflows** (LLM / agent calls). Three contracts
define everything; internalize them before writing any code.

### 1.1 Hot/cold separation

```
your business code
      |
      v
+-------------------+        +------------------------+
|   HOT PATH (fast) |        |   COLD PATH (durable)  |
|  Redis + Lua      |        |  local file / adapters |
|                   |        |                        |
| - task state      |  -->   | - snapshots (every     |
| - semaphore slots | write  |   execution generation)|
| - quota counters  |        | - audit events         |
| - cancel flags    |        | - dependency edges     |
+-------------------+        | - content blobs        |
                             | - result manifests     |
                             +------------------------+
                  the "trace bundle"
```

- **Hot path** = transient state of in-flight tasks. Millisecond-sensitive,
  pinned to Redis + Lua (never abstracted). Owned by `orditect-core`.
- **Cold path** = the durable, queryable record. Owned by
  `orditect-protocol` contracts; `orditect-adapter-local` implements it as
  plain files (the **trace bundle**). You read it with
  `orditect-adapter-ui`.

**Rule**: business code touches the hot path only through
`orditect-flow` APIs; it reads history only through the cold path. Never
mix the two.

### 1.2 Mechanism to the framework, semantics to the business

The framework embeds **no business vocabulary**. Every status word,
success word, event type, backend name is an opaque string **you declare**.

Concrete consequence — these constructors reject empty vocabularies:

- `RecoveryService(reuse_terminal_words=frozenset({"succeeded"}))`
- `DependencyGovernor(success_words=frozenset({"succeeded"}))`
- `TaskRedisDB(terminal_statuses=(...), transitions={...})`

If you hit `ValueError: ... requires explicit non-empty ...`, that is the
framework enforcing vocabulary neutrality (T6), not a bug.

### 1.3 Observation never blocks (T9)

Every observation write (snapshot sink, audit, hooks) is wrapped in
try/except by the framework: a failing sink logs a warning and the task
keeps running. You will see lines like
`snapshot write failed (execution unaffected): ...` — read them as
**observation degradation, not task failure**.

### The three failure honesties (memorize to avoid misreading logs)

| What you see | What it means |
|---|---|
| `Traceback ... RuntimeError: simulated first-run failure` | a task failed and was **governed** (logged with `exc_info=True`). The workflow continues. Not a crash. |
| `snapshot write failed (execution unaffected)` | observation degraded (T9). Task itself is fine. |
| `data-rules: 0 violations, 1 warnings` | `run_rules` warning = legally-possible-but-review state (e.g. a dangling reference that may be T1-expired). Warnings never fail the bundle. |

---

## Chapter 2 — Your first workflow in 5 minutes (zero infrastructure)

The `examples/mvp` demo runs the full governance loop with **no Redis, no
API keys, no services**. Start here to build intuition.

### 2.1 Run it

```bash
cd <repo root>
pip install -r examples/mvp/requirements.txt
python examples/mvp/run_demo.py
```

You will see, in order:

1. **workflow**: a recursive pipeline `collect -> analyze -> report`
   (the report node fails on its first generation, on purpose);
2. **visualization**: lineage tree, execution generations, dependency
   graph, audit events, aggregate stats;
3. **HITL retry**: `retry_scope` reopens the failed node on a new
   generation and it succeeds;
4. **HITL pause + resume**: a slow node is cancelled, then the tree is
   resumed — succeeded nodes are reused, only the cancelled node reruns;
5. **validation**: `run_rules` reports `0 violations` over the produced
   trace bundle.

### 2.2 What each piece is

| Console output | Produced by | Orditect piece |
|---|---|---|
| `pipeline finished: succeeded` | flow orchestrator | `TaskOrchestrator` (recursive composition) |
| `=== workflow tree ===` | adapter-ui reader | `TraceBundleReader.snapshot.get_tree` |
| `=== execution generations ===` | adapter-ui reader | `get_tree(latest_only=False)` — time travel |
| `[llm_call] id=... tokens=30` | bridge | `GovernedLLMClient` (mock endpoint) |
| `action executed: ... rerun=1 of 4` | flow dispatcher | `ActionDispatcher` consuming the action queue |
| `data-rules: 0 violations` | protocol rules | `run_rules` over the trace bundle |

### 2.3 The single most important line (the visualization switch)

Inside `run_demo.py`:

```python
orchestrator = TaskOrchestrator(
    storage,
    governor,
    snapshot_sink=ProtocolSnapshotSink(store.snapshot),
)
```

**Without `snapshot_sink`, the executor uses a NullSink and the trace
bundle stays EMPTY** — no tree, no generations, nothing to visualize.
This is the #1 reason developers think adapter-ui is broken. Always wire
a snapshot sink when you want observability.

### 2.4 The demo's hot/cold split (why it runs with zero infra)

The MVP replaces the production hot path with three in-memory doubles
(`examples/mvp/infra.py`):

| Double | Duck-typed protocol | Production counterpart (Ch.3) |
|---|---|---|
| `InMemoryTaskStorage` | flow `TaskStorageProtocol` + `reopen_task` | `TaskRedisDB` |
| `InMemoryGovernor` | flow `ResourceGovernorProtocol` | `AsyncLeaseSemaphore` via `LimiterRegistry` |
| `InMemoryQuota` | `BudgetLedger`'s quota DB | `AdmissionQuotaRedisDB` |

The **cold path is real** from the start: `LocalFileStore` writes an
actual trace bundle to `examples/mvp/demo_data/trace/`. Swapping the
three doubles for Redis (next chapter) requires **zero business-code
change** — that is the replaceability thesis.

### 2.5 Files to study (in this order)

1. `examples/mvp/run_demo.py` — the wiring (setup -> workflow -> HITL ->
   validate);
2. `examples/mvp/tasks.py` — task definitions (`BaseBackEndTask`
   subclasses) and the `task_factory`;
3. `examples/mvp/viewer.py` — the five read views over the trace bundle;
4. `examples/mvp/infra.py` — the hot-path doubles (skip on first read).

### 2.6 Where the data lives

```
examples/mvp/demo_data/trace/
  snapshots.ndjson   # every execution generation (op envelope rows)
  audit.ndjson       # llm_call + action_* events (append-only)
  deps.ndjson        # dependency edges
  results/<id>.json  # result manifests
  content/sha256/..  # pointer-ized content blobs
```

These are plain JSON/ndjson files — you can `cat` them, `jq` them, or
read them with `TraceBundleReader` (Chapter 6). Nothing is hidden.

---

## Chapter 3 — Moving to production: the Redis hot path

`examples/real-world` runs the **identical business code** as the MVP
against the production hot path (Redis) and a real OpenAI-compatible LLM.
This chapter is the swap manual.

### 3.1 The swap table (the only thing that changes)

| Role | MVP (in-memory) | Production (Redis) |
|---|---|---|
| Task store | `InMemoryTaskStorage()` | `get_default_storage(client)` -> `TaskRedisDB` |
| Semaphore | `InMemoryGovernor()` | `TaskbaseGovernorAdapter(get_registry())` over `AsyncLeaseSemaphore` |
| Quota | `InMemoryQuota()` | `AdmissionQuotaRedisDB(client=client)` |
| LLM endpoint | `httpx.MockTransport` | `GovernedLLMClient(base_url, api_key=...)` |
| **Business code (`tasks.py`, workflow, HITL calls)** | — | **identical** |

The real-world `infra.py::build_hot_path()` is the whole swap:

```python
import redis.asyncio as aioredis
from orditect.core import AdmissionQuotaRedisDB, get_registry
from orditect.flow.governor.factory import TaskbaseGovernorAdapter
from orditect.flow.storage.factory import get_default_storage

async def build_hot_path(redis_url: str, llm_sem_limit: int = 30):
    client = aioredis.from_url(redis_url, decode_responses=True)
    await client.ping()  # fail fast on a bad REDIS_URL

    storage = get_default_storage(client)   # TaskRedisDB (flow vocab wired)
    await storage.connect()                  # registers the Lua scripts

    registry = get_registry()
    registry.register_semaphore("llm", client, limit=llm_sem_limit, lease_time=60.0)
    registry.register_semaphore("task_execution", client, limit=10, lease_time=60.0)
    governor = TaskbaseGovernorAdapter(registry)

    quota = AdmissionQuotaRedisDB(client=client)
    await quota.connect()
    return storage, governor, quota, client
```

Everything after this — `TaskOrchestrator`, `BudgetLedger`,
`RecoveryService`, the tasks themselves — is byte-for-byte the MVP code.

### 3.2 Semaphore resources you must register

The executor acquires a token per node under the resource name
`task.resource_type` (default `"task_execution"`); your governed call
sites use their own names (e.g. `"llm"`). **R16 discipline: a resource
must be registered before first acquire, or you get `KeyError`.**

```python
registry.register_semaphore("task_execution", client, limit=10)   # task boundary
registry.register_semaphore("llm", client, limit=30)              # LLM call site
registry.register_semaphore("vector_search", client, limit=5)     # any custom resource
```

### 3.3 Configuration discipline (.env)

Real endpoints and credentials live in `.env`, never in code:

```bash
# examples/real-world/.env.example — copy to .env and edit
LLM_BASE_URL=http://localhost:11434/v1   # any OpenAI-compatible endpoint
LLM_API_KEY=ollama
LLM_MODEL=qwen2.5:7b
REDIS_URL=redis://localhost:6379/0
LLM_SEM_LIMIT=30
BUDGET_MAX_UNITS=100000
```

Rules:

- `.env.example` is committed; **`.env` is gitignored** (both at repo
  root and in the example dir). Never commit a real key.
- `settings.py` loads with safe defaults so the CI smoke test runs
  without a `.env`.

### 3.4 Redis hygiene for demos

The demo uses the default prefixes (`task:*`, `task_status:*`,
`{ftb}:semaphore:*`, `admission:*`). Two options to avoid colliding with
other data:

- point `REDIS_URL` at a dedicated logical DB (`redis://localhost:6379/14`);
- or parameterize `TaskRedisDB(task_key_prefix=..., status_index_prefix=...)`
  and the semaphore `key_prefix` (advanced; see core docs).

The budget ledger scope is `budget:{root_task_id}`. If you run several
demos against one Redis, give each a distinct `root_task_id` (e.g.
`pipeline-root-stream`) or clean the residue:

```bash
redis-cli -u "$REDIS_URL" --scan --pattern "admission:budget:<scope>:*" \
  | xargs -r redis-cli -u "$REDIS_URL" del
```
> **Resource discipline**: every semaphore name you `acquire` must be
> registered first (R16). If you add custom call sites (OCR, vector
> search, image ops), register them in `build_hot_path()` exactly like
> `"llm"` and `"task_execution"`, or the first acquire raises `KeyError`.

### 3.5 Real LLM notes

- **Timeout**: `GovernedLLMClient(..., timeout=120.0)` covers slow real
  calls; your `wait_terminal(step, timeout=...)` must exceed a single
  call's worst latency (a 10s default will spuriously fail a 30s call).
- **Usage-driven budget**: audit events now carry the endpoint's real
  `usage` (token counts) and `elapsed_ms`; `BudgetLedger` deducts real
  tokens instead of the mock's fixed value.
- **Endpoints without `stream_options`**: some OpenAI-compatible servers
  (certain Ollama builds) silently return an empty body when
  `stream_options.include_usage` is present. `GovernedLLMClient.stream()`
  accepts `include_usage=False` for those; the stream then works and
  `cost_fn` receives `None` (A5: your business prices the usage-missing
  call).

---

## Chapter 4 — Writing your own task node

Everything you execute is a `BaseBackEndTask` subclass. This chapter is
the authoring template.

### 4.1 The minimal node

```python
from orditect.flow import BaseBackEndTask

class ResizeImageTask(BaseBackEndTask):
    # Multi-type governance: this node's semaphore resource name.
    # Default is "task_execution"; override to govern different node
    # types with different pools (must be registered, see Ch.3.2).
    resource_type = "image_ops"

    async def execute(self, task_id: str, **kwargs) -> dict:
        image_key = kwargs["image_key"]
        # ... do the work ...
        return {"resized_key": "s3://bucket/resized.jpg"}
```

Submit it:

```python
task_id = await orchestrator.submit(
    ResizeImageTask(storage),
    image_key="s3://bucket/original.jpg",
)
record = await orchestrator.wait_terminal(task_id, timeout=60)
print(record["status"], record.get("result"))
```

### 4.2 The executor lifecycle (what happens around execute())

Understanding this explains every snapshot/status you will later read:

1. **R5 pre-check**: if `cancel_requested` is already set, raise
   `CancelledError` immediately (never acquires a slot);
2. **F3 reuse check** (optional): if the node's latest generation already
   succeeded and the hot record carries a result, **short-circuit and
   reuse it** — no slot acquired, no re-execution (Chapter 12);
3. **acquire** the `resource_type` semaphore (skipped when an ancestor
   holds the same resource — lineage exemption, Ch.5);
4. write status `running` + a `save(running)` snapshot;
5. run your `execute()`;
6. on success: write `succeeded` + `save_terminal(succeeded)` snapshot;
   on failure: `failed`; on cancellation: `cancelled`;
7. release the slot (shielded — a second cancel cannot swallow it).

Every status word here (`running`/`succeeded`/`failed`/`cancelled`) is
the **flow default vocabulary**; the snapshot rows these produce are what
`TraceBundleReader` later renders.

### 4.3 Cooperative cancellation (long-running nodes)

If your node runs long, check the cancel flag at segment boundaries so a
`pause` actually interrupts it (mirrors the core `CancellationToken`
discipline):

```python
class SlowTask(BaseBackEndTask):
    async def execute(self, task_id: str, **kwargs) -> dict:
        for chunk in self._work_items:
            record = await self.storage.get_task(task_id)
            if record.get("cancel_requested"):
                raise asyncio.CancelledError()  # executor settles as cancelled
            await process(chunk)
        return {"done": True}
```

A node that never checks the flag runs to completion even after a pause;
the executor then settles it as `cancelled` with
`cancel_outcome = "succeeded_but_cancelled"` (the 1c discipline).

### 4.4 Governed call sites inside a node (GovernedClient)

A task often makes several governed calls (LLM, OCR, OSS). Wrap each call
site with `GovernedClient` — acquire at call start, release at call end,
independent of the task-level slot:

```python
from orditect.flow import GovernedClient

class AgentTask(BaseBackEndTask):
    resource_type = "task_agent"

    async def execute(self, task_id: str, **kwargs) -> dict:
        governor = kwargs.get("governor")   # injected by the executor
        llm = GovernedClient(governor, resource="llm", handler=call_llm,
                             timeout=60.0)
        return await llm.call(kwargs["prompt"])
```

Two-layer governance, memorized:

- **task layer** (`resource_type`): held for the node's whole lifetime;
- **call layer** (`GovernedClient(resource=...)`): held per call.

They are independent pools and compose freely.

### 4.4a A runnable recipe (the community integration entry point)

`examples/governed-client` is a single-file, zero-infrastructure demo of
this exact pattern: `GovernedClient` and `GovernedCallClient` wrapping a
plain callable, with `cost_fn` pricing, `call_id` dual-habitat
idempotency, the streaming semaphore lifecycle, and the usage-missing
(A5) pricing path. If you only read one example before embedding
Orditect into existing code, read that one.

### 4.5 Registering the node for recovery (task_factory)

`RecoveryService` cannot reconstruct your task from a `task_id` — that is
business semantics. Register every rerunnable node in a factory:

```python
async def task_factory(task_id: str):
    if task_id == "resize":
        return ResizeImageTask(storage)
    if task_id == "agent":
        return AgentTask(storage)
    raise KeyError(f"unknown task_id: {task_id}")
```

A node missing from the factory reruns as
`recovery rerun failed: <id>, 'unknown task_id: <id>'` (logged, never
fatal — the rest of the tree still recovers).

### 4.6 Result, error, and progress fields

- `return value` -> hot record `result` (reused by F3/recovery);
- `raise` -> hot record `error` + `failed` status;
- `await self.report_progress(task_id, 0.5)` -> hot record `progress`
  (optional; queryable via `orchestrator.get_task`).
---

## Chapter 5 — Recursive composition (nested workflows)

Orditect's signature capability: any task can submit child tasks inside
its own `execute()`, and children can submit grandchildren — depth is
limited only by your business logic. Three iron rules govern it.

### 5.1 Iron rule 1 — lineage is automatic (contextvar)

When a task calls `orchestrator.submit()` inside `execute()`, the child's
`parent_task_id` is **injected automatically** — zero boilerplate:

```python
class ParentTask(BaseBackEndTask):
    async def execute(self, task_id: str, **kwargs) -> dict:
        # No parent_task_id needed: the executor's task context provides it.
        child_id = await self._orch.submit(ChildTask(self.storage))
        await self._orch.wait_terminal(child_id, timeout=60)
        return {"child": child_id}
```

Mechanism: `asyncio.create_task` copies the context; the executor sets a
`current_task_id` contextvar before running your `execute()`, and
`submit()` reads it when `parent_task_id` is not given explicitly.

- **Explicit wins**: `submit(..., parent_task_id="x")` overrides the
  context value;
- **Top-level submit** (outside any task) has no context -> the node is a
  tree root.

This is what makes the lineage tree in Chapter 6 appear with no effort.

### 5.2 Iron rule 2 — cancellation cascades

`cancel()` and `terminate()` recurse along `parent_task_id` — no orphan
tasks:

| API | Mode | Effect |
|---|---|---|
| `orchestrator.cancel(id)` | graceful | sets `cancel_requested` on the node **and every descendant** (running nodes finish their segment, then settle as cancelled) |
| `orchestrator.terminate(id)` | force | additionally cancels the running coroutine in this process (semaphore released immediately) |

Cascade skips nodes already in a terminal state and is depth-capped
(`_MAX_CASCADE_DEPTH = 32`) so a lineage cycle cannot loop forever.

### 5.3 Iron rule 3 — resource lineage exemption (no self-deadlock)

The classic recursive trap: parent holds `task_execution` (capacity 1),
child needs `task_execution` too -> deadlock. Orditect solves it with the
**resource ledger + ancestor walk**:

- on acquire, the executor records the node's `resource` in its hot
  record;
- before acquiring, it walks up the lineage: if **any ancestor** holds a
  slot under the same resource name, the child is **exempt** (inherits the
  ancestor's slot, acquires nothing, releases nothing).

Consequences you can rely on:

- same-resource nesting at any depth costs **one** slot total (the
  top-level one);
- different-resource nesting acquires normally (parent's `task_execution`
  + child's `llm` are two independent pools);
- exemption never releases the ancestor's slot (the child marks the slot
  as inherited).

If a node's `exempt_resources_snapshot` was frozen at dependency
registration (Chapter 8), that snapshot takes precedence over the live
walk; `invalidate_exempt_snapshot` resets it on reopen.

### 5.4 Waiting for children (wait_terminal)

Inside a parent, wait for a child to reach a terminal state:

```python
record = await self._orch.wait_terminal(child_id, timeout=300)
if record["status"] == "succeeded":
    return record["result"]
```

Notes:

- Polling uses exponential backoff (x1.5, capped at 0.5s) — long tasks do
  not hammer Redis;
- `timeout` must exceed a single child's worst latency (a 10s timeout
  spuriously fails a 30s LLM call);
- `TimeoutError` (builtin) on expiry, `TaskNotFoundError` for a ghost id.

### 5.5 Idempotent submit (parent retry safety)

If a parent may be retried, prevent re-submitting an already-running
child:

```python
child_id = await self._orch.submit(
    ChildTask(self.storage), task_id="child-1", if_not_exists=True,
)
```

Returns the existing `task_id` and skips all writes when the id already
exists (the check-and-write is atomic in `task_init.lua` — exactly one
winner under concurrency).

---

## Chapter 6 — Observability: reading the cold path

Everything the workflow did is queryable from the trace bundle via
`orditect-adapter-ui`'s `TraceBundleReader` — without importing
orditect-core/flow internals.

```python
from orditect.adapter.ui import TraceBundleReader

reader = TraceBundleReader("path/to/trace")   # the LocalFileStore directory
```

The reader loads the bundle at construction time; build a fresh one for
the latest data (cheap at this scale).

### 6.1 The five views

**1. Lineage tree (latest generation per node)**

```python
tree = await reader.snapshot.get_tree(root_id, latest_only=True)
for s in tree:
    print(s.task_id, s.parent_task_id, s.status, s.execution_id)
```

**2. Execution generations (time travel)**

```python
full = await reader.snapshot.get_tree(root_id, latest_only=False)
# A retried node appears multiple times:
#   report  exec-aaa:failed  ->  exec-bbb:succeeded
```

Every `reopen` (retry/rerun) produces a **new `execution_id`**; the old
generation's rows are never mutated (T3). This is the data foundation of
resume/replay auditing.

**3. Status-filtered query + aggregate**

```python
failed = await reader.snapshot.query(status="failed")
stats = await reader.snapshot.aggregate(group_by="status")
# stats == {"succeeded": {"count": 3, "cost": {}}, "failed": {"count": 1, ...}}
```

`sort.field` / `group_by` outside the mechanism whitelist raise
`InvalidQueryError` (never silently fall back). Whitelists (mechanism
fields only): snapshot sort in `created_at/updated_at/expire_at`, group
by `status/model`; audit sort in `created_at/event_id`.

**4. Audit events**

```python
rows = await reader.audit.query(task_id="analyze")
for e in rows:
    print(e.event_type, e.event_id, e.payload.get("usage"), e.payload.get("elapsed_ms"))
```

You will see `llm_call` events (bridge writes: usage, elapsed_ms,
cost_units) and `action_pause` / `action_retry` / `action_resume` events
(HITL writes, `event_id == action_id`).

**5. Dependency graph (see Chapter 7 for its exact semantics)**

```python
graph = await reader.dependency.read_graph(root_id)
print(graph.task_ids, [(e.child_id, e.parent_id, e.is_primary) for e in graph.edges])
```

### 6.2 Result manifests and content blobs

```python
manifest = await reader.result.get(stream_id)   # None when missing/expired
```

Large bodies are pointer-ized (T5): snapshots/audit carry `TaskPointer`
records (`backend`, `key`), never the payload itself. The bytes live
under `content/sha256/<aa>/<digest>` in the bundle (content-addressed —
identical content dedups naturally).

### 6.3 Reading the bundle without any orditect import

The trace bundle is plain files — any tool can consume it:

```bash
jq -c 'select(.data.status=="failed")' trace/snapshots.ndjson
jq -s 'length' trace/audit.ndjson
```

Row shape (op envelope, stable since v0.1):

```json
{"v": 1, "op": "save_terminal", "ts": "2026-08-31T06:20:57Z",
 "data": {"task_id": "report", "step": "execute",
          "execution_id": "exec-...", "status": "succeeded",
          "created_at": "...Z", "updated_at": "...Z"}}
```

`op` is `save` / `save_terminal` / `append` / `edge_write`; `data` is the
model payload. None-valued fields are omitted (read with `.get()`).

### 6.4 Self-certify your bundle (run_rules)

Validate any produced bundle against the data rules:

```python
import json
from orditect.protocol.rules import run_rules

lines = []
for name in ("snapshots.ndjson", "audit.ndjson", "deps.ndjson"):
    p = f"path/to/trace/{name}"
    lines += [json.loads(x) for x in open(p) if x.strip()]

report = run_rules(lines)
print(report.summary())   # "data-rules: 0 violations, 0 warnings"
assert report.ok          # ok == zero violations (warnings never fail)
```

Rules checked include: T3 terminal drift, T4 idempotency conflicts, T11
execution_id presence, T7 clock offsets, T5 pointer resolution. This is
how you prove your pipeline's output is protocol-compliant — treat it as
the last step of any workflow you ship.
---

## Chapter 7 — Observing the DAG (two views, do not conflate)

"Show me the DAG" is two different questions with two different answers.
Mixing them is the most common observability mistake.

| Question | View | Data source | Answers |
|---|---|---|---|
| **Who depends on whom?** | dependency graph | `DependencyEdge` facts (pure edges) | structure |
| **Where is the run right now?** | snapshot tree | `TaskSnapshot` rows (execution states) | progress |

### 7.1 Dependency graph — the structural view (pure-edge facts, T12)

```python
graph = await reader.dependency.read_graph(root_id)
print(graph.task_ids)          # every reachable node (ids only, T12)
for e in graph.edges:
    print(e.child_id, "depends on", e.parent_id,
          "(primary)" if e.is_primary else "")
```

Properties to rely on:

- **Edges only**: nodes are bare `task_id` references, no properties —
  node state lives in the snapshot domain, never duplicated here (no
  dual-write drift);
- **Binds the task, not the generation**: a rerun (new `execution_id`)
  never rewrites edges;
- **Cycle-safe by construction**: reads use a visited set, so a cycle in
  the data terminates traversal rather than looping (the store records
  facts; cycle *detection* is an offline tool, see `scan_dependency_cycles`);
- **Closure, both directions**: `read_graph` returns the transitive
  neighbourhood (upstream "what I depend on" + downstream "who depends on
  me"); `children_of` / `parents_of` give one-level neighbours.

**What it does NOT tell you**: which nodes have run, which are running,
which failed. That is not this view's job.

### 7.2 Snapshot tree — the execution-state view

```python
tree = await reader.snapshot.get_tree(root_id, latest_only=True)
for s in tree:
    print(s.task_id, s.status)   # running / succeeded / failed / cancelled
```

This is "where the run is": each node's **latest generation** with its
status. Combine with `get_tree(latest_only=False)` for the full
time-travel (every generation).

### 7.3 How the edges get written

Edges are facts someone must record. Two sources:

- **You, explicitly** (as the demos do):
  ```python
  from orditect.protocol import DependencyEdge
  await store.dependency.write_dependency(
      DependencyEdge(child_id="analyze", parent_id="collect", is_primary=True)
  )
  ```
- **DependencyGovernor** (Chapter 8), when you wire it with a cold
  `dep_graph_store` — it writes edges as part of multi-parent
  registration.

The lineage tree (parent_task_id) and the dependency graph are **two
independent projections**: a child submitted inside `execute()` appears
in the snapshot tree automatically, but only appears in the dependency
graph if an edge was written. For a linear pipeline you typically write
edges yourself (one line per edge); for multi-parent fan-in, use
DependencyGovernor.

### 7.4 A 10-line read-only endpoint (reference UI)

The framework deliberately ships no frontend. To expose these views to a
React/Vue app, wrap the reader — this is the entire integration:

```python
from fastapi import FastAPI
from orditect.adapter.ui import TraceBundleReader

app = FastAPI()
BUNDLE = "path/to/trace"

@app.get("/tree/{root_id}")
async def tree(root_id: str):
    r = TraceBundleReader(BUNDLE)
    snaps = await r.snapshot.get_tree(root_id, latest_only=True)
    return [s.to_payload() for s in snaps]

@app.get("/graph/{root_id}")
async def graph(root_id: str):
    r = TraceBundleReader(BUNDLE)
    g = await r.dependency.read_graph(root_id)
    return g.to_payload()

@app.get("/audit")
async def audit(task_id: str | None = None):
    r = TraceBundleReader(BUNDLE)
    rows = await r.audit.query(task_id=task_id)
    return [e.to_payload() for e in rows]
```

Render it however you like — that is your product layer.

---

## Chapter 8 — Dependency governance (the driving side, multi-parent)

`DependencyGovernor` governs **multi-parent dependency relationships**
for tasks whose readiness depends on several parents completing. It is a
**passive, protocol-layer API**: it registers relationships, answers
readiness, collects cancel votes, and processes terminal notifications.
It never creates tasks, never schedules execution, never interprets DAG
semantics — those stay with you (or your external orchestrator).

> Most first workflows (linear pipelines, recursive composition) do NOT
> need this chapter. Read it when you have fan-in: "C runs only after A
> AND B finish."
> The runnable companion for this chapter is
> [`examples/dependency-governance`](dependency-governance/) — it drives the
> full register / notify / readiness / voting lifecycle with zero
> infrastructure, including the hang-prevention vote path and the
> graph-vs-tree observability distinction from Chapter 7.

> The runnable companion for this chapter is
> [`examples/dependency-governance`](dependency-governance/) — it drives
> the full register / notify / readiness / voting lifecycle with zero
> infrastructure, including the hang-prevention vote path and the
> graph-vs-tree observability distinction from Chapter 7.

### 8.1 Construction (vocabulary is caller-declared)

```python
from orditect.flow.governance import DependencyGovernor

gov = DependencyGovernor(
    storage,                                    # task store (TaskRedisDB or the MVP double)
    success_words=frozenset({"succeeded"}),     # REQUIRED, non-empty (T6)
    terminal_words=frozenset({"succeeded", "failed", "cancelled"}),  # optional
    ready_status="pending",                     # optional
    lifecycle=orchestrator.lifecycle,           # optional; vote-triggered cancel
    audit_writer=store.audit,                   # optional
    dep_graph_store=store.dependency,           # optional cold path (T8)
)
```

Without `dep_graph_store`, the hot path works fully and
`get_dependency_graph()` raises `UnsupportedCapabilityError` — explicit
capability (T8), not a bug.

### 8.2 The three-call lifecycle

```python
# 1. After creating the child task (YOUR job): register its parents.
await gov.register_dependency("c", ["p1", "p2", "p3"])

# 2. After ANY task reaches a terminal state (YOUR job): notify.
await gov.notify_task_terminal("p1", "succeeded")

# 3. Ask what is ready (read-only; schedules nothing).
ready = await gov.get_ready_tasks()   # ["c"] once remaining_deps <= 0
```

The governor never drives execution: when `get_ready_tasks()` reports
`["c"]`, **you** decide to submit `c`.

### 8.3 Parent classification at registration

When `register_dependency("c", parents)` runs, each parent is classified
by its current status:

| Parent status | remaining_deps | active_children | cancel_votes |
|---|---|---|---|
| non-terminal | +1 | SADD | — |
| terminal & success | — | — | — |
| terminal & not success | — | — | SADD (pre-cast vote) |

So registering after some parents already finished is legal and correct.

### 8.4 The voting discipline (hang prevention, pinned)

- **Success never auto-votes** — a succeeded parent only decrements the
  child's counter;
- **Abnormal terminals auto-vote** — a failed/cancelled parent casts a
  cancel vote on the child (prevents the child hanging forever on a dead
  dependency);
- **Threshold = all parents**: when votes reach `len(parents)`, the
  child is cancelled via the injected `lifecycle`;
- **Atomicity**: votes are cast and counted in one MULTI/EXEC — exactly
  one concurrent caller observes the threshold and triggers cancellation;
- `vote_cancel(parent_id, child_id)` is the manual entry (e.g. a parent
  deciding a sibling's failure should also cancel the child early).

### 8.5 Terminal notification (the unified entry, both directions)

`notify_task_terminal(task_id, terminal_status)` handles both roles of
the task:

- **as a parent**: for each active child — decrement its counter
  (success: no vote; abnormal: auto-vote);
- **as a child**: remove itself from every parent's active set and clear
  its own vote set.

**You must call it for EVERY terminal task, not only parents** — the
governor needs the as-child direction for cleanup. It is never called by
the built-in executor: wiring it at your task-closure points (or in your
bridge) is your responsibility. All its failures are logged, never raised
(observation never blocks, T9).

### 8.6 Readiness semantics and fault tolerance

- Ready = `remaining_deps <= 0` **and** `status == ready_status`;
- A `remaining_deps` going negative (duplicate notify, DECR on a missing
  counter) is tolerated — readiness is a `<= 0` threshold, and the
  anomaly is logged as a warning, not an error;
- If you never call `notify_task_terminal`, counters never move and the
  child stays pending — **that is the caller's contract, no compensation
  exists**. Wire it at every task-closure point (the demos show the
  pattern: submit -> wait_terminal -> notify).
- After a Redis restart, counters/sets can be rebuilt from the cold store:
  `rebuild_dep_counters(storage, dep_graph_store)` (see
  `flow/docs/governance.md` for its report fields).



### 8.7 The exemption snapshot (advanced, one paragraph)

Registration freezes an `exempt_resources_snapshot` on the child (an
explicit list, or inherited along the primary-parent chain). At execution
time this snapshot takes precedence over the live ancestor walk for
resource exemption (Ch.5.3). After a `reopen` (new generation), call
`gov.invalidate_exempt_snapshot(child_id)` — `RecoveryService`'s rerun
does this automatically when a governor is injected.

---

## Chapter 9 — The output plane: streaming to end users (SSE)

Everything so far produced results for **you** (the operator). This
chapter is about producing output for **your end users**: streaming an
LLM response live over SSE using `orditect-stream` — while the producing
node stays a fully governed flow task. See `examples/stream`.

### 9.1 Output plane vs DAG observability (do not conflate)

| | Output plane (this chapter) | DAG observability (Ch.6-7) |
|---|---|---|
| Audience | your **end users** | you (operator/developer) |
| Data | live `stream.delta` token chunks | persisted snapshots/edges/audit |
| Mechanism | `orditect-stream` SSE protocol | `TraceBundleReader` over the trace bundle |
| Lifetime | one streaming session | durable, queryable forever |

A node can do both at once: stream to users AND write snapshots — that is
exactly what `examples/stream/tasks.py::StreamAnalyzeTask` does.

### 9.2 The minimal streaming node

```python
from orditect.stream import (
    DEFAULT_CONFIG, EnrichMode, MockVectorEnricher,
    SourceRequest, StageConfig, SourceType, StreamRunner,
)
from orditect.stream.store import get_protocol_store

class StreamAnalyzeTask(BaseBackEndTask):
    async def execute(self, task_id: str, **kwargs) -> dict:
        source = MyLLMSource(self._llm)          # an LLMSourceProtocol
        result_store = get_protocol_store(self._store.result, self._store.result)
        runner = StreamRunner(
            stages=[StageConfig(
                name="analyze",
                source_type=SourceType.LLM,
                source=source,
                request=SourceRequest(payload={"messages": [...]}),
            )],
            enricher=MockVectorEnricher(),
            store=result_store,                   # manifest lands in the trace bundle
            config=DEFAULT_CONFIG.merge(enrich_mode=EnrichMode.LOCAL),
            loading_url="https://oss.example.com/loading.jpg",
        )
        collected: list[str] = []
        async for envelope, event_type in runner.run():
            # forward (envelope, event_type) to your SSE response / stdout
            if event_type.value == "stream.delta" and envelope.data.get("kind") == "content":
                collected.append(envelope.data.get("text", ""))
        return {"analysis": "".join(collected)}   # task result stays intact
```

Key point: the runner's aggregated content is **also** returned as the
task result, so the snapshot/result/recovery chain (Ch.6, Ch.12) keeps
working — streaming is additive, never a replacement for governance.

### 9.3 The event sequence (protocol-frozen, golden-tested)

A healthy stream emits, in order:

```
stream.start            stages=[...]
stream.delta            {"kind": "content", "text": "..."}     (many)
stream.delta            {"kind": "thinking", "text": "..."}    (optional)
enrich.marker           {"placeholder_id": "ph_..."}           (when ![img] detected)
enrich.placeholder      {"placeholder_id": ..., "loading_url": ...}
enrich.resolved         {"placeholder_id": ..., "url": ...}    (within settle window)
stage.end               {"name": "analyze"}
stream.manifest         {"stages": ..., "placeholders": [...], ...}
stream.end
```

Rules consumers depend on:

- `seq` is monotonic per `stream_id`; `id: {stream_id}:{seq}` is the
  resume anchor (parsing reserved);
- optional fields are omitted when None — always `.get()`;
- **`stream.end` is the only terminal signal**, including on cancel.

### 9.4 Rich-media placeholders (the signature feature)

When the model emits the marker `![img]`, the framework:

1. detects it mid-stream (split-marker safe across chunk boundaries),
2. emits `enrich.marker` + `enrich.placeholder` (with `char_offset` —
   the exact insert position for reassembly),
3. dispatches an enrich task (your `EnricherProtocol` — vector search,
   image generation, ...),
4. backfills `enrich.resolved` within the settle window, or records the
   placeholder in the manifest with its `task_ref` for the client to
   resolve later.

`MockVectorEnricher` is the development stub; production enrichers are
your own (out of this guide's scope, like other adapters).

### 9.5 The governed source (LLMSourceProtocol)

Any object with `async def stream(request, cancel_token=None)` yielding
`SourceChunk(text=...)` and ending with `SourceChunk(finish=True)` works
as a source. Two ready options:

- `GovernedLLMClient.stream()` — governance built in (semaphore/budget/
  audit); pass `include_usage=False` for endpoints that reject
  `stream_options` (some Ollama builds);
- a raw httpx stream wrapped in `GovernedCallClient.call_streaming` —
  for full control over the wire format.

**call_id caveat (idempotency)**: `SourceRequest.payload` is an opaque
business dict — the runner never interprets it, so a `call_id` placed
there does NOT become the dual-habitat idempotency key (it would only
leak into the HTTP request body). The key must travel via the source's
own public channel, e.g. `GovernedLLMClient.stream(call_id=...)`. When
you wire `GovernedLLMClient` as a stage source, map it explicitly:

```python
class MySource:
    def __init__(self, llm) -> None:
        self._llm = llm

    async def stream(self, request: SourceRequest, cancel_token=None):
        async for chunk in self._llm.stream(
            messages=request.payload["messages"],
            call_id=request.payload.get("call_id"),   # public kwarg
            include_usage=False,
        ):
            yield chunk
```

(`examples/stream/tasks.py::_GovernedSource` is this exact shape, plus
an async wrapper for the stream-side sync CancellationToken — see Ch.9.6.)

`SourceChunk` fields: `text` (content delta), `thinking` (reasoning
delta), `references` (citation list), `finish` (terminal flag). A chunk
may carry several fields; the pipeline splits them
(references -> thinking -> text order).

### 9.6 Cancel semantics (Ctrl+C vs programmatic force)

- **Graceful (default)**: `runner.cancel(stream_id=sid)` — stop output,
  LLM connection kept, semaphore held until the LLM truly ends, partial
  content preserved. This is what a Ctrl+C/user-interrupt should do.
- **Force**: `runner.cancel(stream_id=sid, force=True)` — cancel the
  executor coroutine, semaphore released immediately, connection dropped.
  Use under resource pressure or a hung LLM.

Either way, `runner.get_partial_content(sid)` returns everything produced
up to cancellation — nothing produced is ever lost. On cancel the client
still receives `stream.cancelled` (with `partial_content`) ->
`stream.manifest` -> `stream.end` (terminal-signal discipline holds).

### 9.7 thinking modes and disconnect policies (one-liners)

- `ThinkingMode.INLINE` (thinking as deltas) / `SEPARATE` (aggregated
  into `stage.end.result.thinking`) / `SUPPRESS` (dropped);
- `DisconnectPolicy.CANCEL` (cascade on disconnect) / `GRACE`
  (grace-period buffer, drain on reconnect) / `CONTINUE` (run to
  completion, refetch the manifest later).

---

## Chapter 10 — HITL: human / MCP / agent intervention

Intervention is **queue-shaped** by design (DD-013): UI, humans, MCP
tools, and agents never touch the hot path directly. They enqueue an
action command; a dispatcher executes it asynchronously. Every action
doubles as an audit event.

### 10.1 The wiring (three objects, one line each)

```python
from orditect.adapter.ui import ActionSinkAdapter, MemoryActionQueue
from orditect.flow.actions import ActionDispatcher

queue = MemoryActionQueue()                       # in-memory (demo); Redis-backed in production
sink = ActionSinkAdapter(queue, audit_writer=store.audit)
dispatcher = ActionDispatcher(queue, orchestrator, recovery)
await dispatcher.start()
```

Production note: `MemoryActionQueue` is the reference implementation
(single process, bounded receipt retention). A production deployment uses
a Redis-backed queue polled by the dispatcher — the sink API is identical.

### 10.2 The three actions

```python
# Pause a running node (graceful cancel + cascade)
receipt = await sink.pause_node("slow-node", actor="user-1")

# Retry an explicit node set inside a tree (reopen new generations)
receipt = await sink.retry_scope(root_id, {"report", "analyze"}, actor="user-1")

# Resume a tree: reuse succeeded nodes, rerun the rest
receipt = await sink.resume_tree(root_id, actor="user-1")
```

`receipt.accepted == True` means the command was **enqueued** (not yet
executed). `receipt.action_id` is your handle for the outcome.

### 10.3 Reading the outcome (receipts)

```python
async def wait_receipt(sink, action_id, timeout=10.0):
    import time, asyncio
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        r = await sink.get_receipt(action_id)
        if r is not None:
            return r
        await asyncio.sleep(0.05)
    raise TimeoutError(action_id)

result = await wait_receipt(sink, receipt.action_id)
# {'action_id': 'act-...', 'action_type': 'retry',
#  'status': 'executed', 'detail': 'rerun=1 of 4'}
```

`status` is `executed` / `rejected` / `failed`; `detail` carries the
dispatcher summary.

### 10.4 Semantics you can rely on

- **Audit trail**: every accepted action appends an audit event with
  `event_id == action_id` and `event_type == "action_pause" / "action_retry"
  / "action_resume"` — intervention is fully traceable in the same audit
  log as everything else (Ch.6.1);
- **At-most-once (adjudicated)**: the dispatcher marks an `action_id`
  seen *before* executing, inside a bounded dedup window (default 10000).
  A failed action is NOT retried on re-delivery within the window. If you
  need guaranteed execution, use a persistent queue with explicit retry —
  the in-memory window is best-effort dedup, not a delivery guarantee;
- **retry = reopen, not state regression**: a retried node gets a new
  `execution_id` (T3-safe); the old generation is untouched and visible
  in the time-travel view;
- **resume = per-node decision**: succeeded nodes with a hot-record
  result are reused (no re-execution), everything else reruns — see
  Chapter 12 for the exact rule.

### 10.5 Failure honesty (what the receipts will tell you)

| Receipt / log | Meaning |
|---|---|
| `'status': 'rejected', 'detail': 'task not found or already terminal'` | pause hit a ghost/finished task — safe to ignore or surface to the user |
| `recovery rerun failed: <id>, 'unknown task_id: <id>'` | the node is missing from your `task_factory` (Ch.4.5) — the rest of the tree still recovered |
| `reopen rejected: task <id> is not terminal (current: running)` | resume was issued while the node still runs — wait for it to terminate first |

None of these crash the dispatcher; each is recorded and the loop
continues (best-effort by design).
---

## Chapter 11 — Budget and quota: real metering with a hard stop

`BudgetLedger` is a cross-layer settlement protocol: a parent (or the
run) opens a budget; every governed call charges against it; exhaustion
blocks the next call before it acquires any resource.

### 11.1 The lifecycle

```python
from orditect.flow import BudgetLedger

budget = BudgetLedger(
    quota_db,                          # AdmissionQuotaRedisDB (prod) / InMemoryQuota (mvp)
    root_task_id="pipeline-root",      # scope becomes "budget:pipeline-root"
    max_units=100_000,                 # units are business-defined (tokens, cents, calls)
    task_ttl_sec=86400,                # ledger auto-reclaims after crash
)
await budget.open()                    # registers the ledger lease (idempotent)

await budget.check()                   # pre-call: raises BudgetExhaustedError when balance <= 0
balance = await budget.charge(30, call_id="call-1")   # post-call: deduct actual cost
print(await budget.balance())          # max_units - consumed
```

Semantics you can rely on:

- **Post-charge model** (LLM usage is only known after the call):
  `check()` verifies `balance > 0`, `charge()` deducts the actual cost;
- **Overspend is recorded honestly**: the last charge may take the
  balance negative; every *subsequent* `check()` then blocks;
- **Ledger TTL**: the quota lease self-reclaims after `task_ttl_sec`, so
  a crashed run never leaks budget forever.

### 11.2 call_id — the dual-habitat idempotency key (important)

`call_id` dedups at **both** layers simultaneously:

- **hot path**: the quota store returns `already_reserved` and never
  double-charges;
- **cold path**: the audit event uses `event_id == call_id`, so the audit
  store dedups identically.

Discipline:

- **Same logical call retried -> same call_id** (no double charge, no
  double audit);
- **New logical call -> new call_id**.

The clean pattern (used in the demos): include the `execution_id`, so a
same-generation retry dedups while a reopened generation (a genuine new
attempt) charges anew:

```python
record = await self.storage.get_task(task_id)
eid = record.get("execution_id", "")
result = await self._llm.chat(
    messages=[...],
    call_id=f"analyze-{task_id}-{eid}",
)
```

The framework default `call-{uuid}` (one record per call) is also correct
when you never retry a logical call.

### 11.3 Wiring budget into call sites

`GovernedClient` / `GovernedCallClient` handle check + charge for you:

```python
client = GovernedClient(
    governor, resource="llm", handler=call_llm,
    budget=budget,
    cost_fn=lambda r: r["usage"]["total_tokens"],   # result -> units
)
await client.call(prompt)   # check() -> acquire -> execute -> charge(cost_fn(result))
```

- `cost_fn` receives the call result (or `None` for usage-missing
  streams — your business prices it, the framework never silently
  estimates);
- when `budget` is None, no check/charge happens (backward compatible);
- a blocked call (`BudgetExhaustedError`) never acquires a slot and never
  writes an audit record — it never reached the resource.

### 11.4 Reading the meter

- `await budget.balance()` — remaining units;
- audit events (`llm_call`) carry `usage`, `elapsed_ms`, and
  `cost_units` whenever `cost_fn` was evaluated — the cold-path mirror of
  the hot-path counter.

---

## Chapter 12 — The recovery plane: resume and rerun

`RecoveryService` drives breakpoint-resume and mid-point replay over the
recursive task tree, built on three layers: core `reopen_task` (hot),
protocol snapshot domain (warm), flow reuse short-circuit.

### 12.1 The per-node decision algorithm (the whole idea in one block)

```text
for each node in the subtree (latest generation):
    if latest snapshot status in reuse_terminal_words
       AND the hot record carries a result:
           -> REUSE   (no re-execution; result reused from the hot record)
    else:
           -> RERUN   (reopen_task opens a NEW generation; executor runs it)
```

- **REUSE** is the F3 short-circuit: no slot acquired, no re-execution,
  the prior result returned;
- **RERUN is a new generation, never a state regression**: terminal
  protection (T3) stays unconditional within a generation; `reopen` only
  opens the next one. The old generation remains visible in time travel.

### 12.2 Construction (vocabulary + factory are yours)

```python
from orditect.flow.recovery import RecoveryService

recovery = RecoveryService(
    storage,                              # task store (with reopen_task)
    store.snapshot,                       # protocol SnapshotReader
    orchestrator.executor,
    reuse_terminal_words=frozenset({"succeeded"}),   # REQUIRED (T6): your success words
    task_factory=my_task_factory,         # REQUIRED: task_id -> task instance (Ch.4.5)
)
```

Both are enforced: empty `reuse_terminal_words` -> `ValueError`;
missing factory mapping -> that node logs `unknown task_id` and the rest
of the tree still recovers.

### 12.3 The two primitives

```python
# Breakpoint-resume: decide() per node (reuse succeeded, rerun the rest)
plan = await recovery.resume(root_id)
# -> {"collect": ReuseDecision.REUSE, "analyze": ReuseDecision.REUSE,
#     "report": ReuseDecision.RERUN}

# Mid-point replay: force-rerun an explicit scope; others follow decide()
plan = await recovery.rerun(root_id, scope={"analyze", "report"})
```

Dispatch detail worth knowing: rerun **bypasses `orchestrator.submit`** —
`reopen_task` already reset the node to its initial state, so the
executor drives re-execution directly (re-initializing would be a double
registration). A node being rerun right after a cancel/pause first drains
its previous generation's in-flight finalization before reopening (the
T11 race guard).

### 12.4 What "pause" means here (no suspend primitive)

LLM/agent calls cannot be frozen mid-generation, so Orditect has **no
suspend mechanism** — pause is a composition of existing primitives:

```text
pause  = cancel/terminate at a node boundary   (Ch.10)
resume = recovery.resume / rerun               (this chapter)
```

This is a deliberate design decision (no suspend state word, no
event-wait machinery). The streaming side mirrors it: on pause the SSE
stream ends normally with a manifest; resume opens a new stream.

### 12.5 Execution identity alignment (T11, why time travel works)

`execution_id` is one concept with three projections that must agree:

- **core hot record**: assigned at `initialize_task`, advanced by
  `reopen_task`;
- **flow execution**: every snapshot the executor writes carries the
  current `execution_id`;
- **protocol snapshot domain**: versions key on
  `(task_id, step, execution_id)`.

You never manage `execution_id` yourself — but this alignment is why
`get_tree(latest_only=False)` shows clean per-generation histories
(`exec-aaa:failed -> exec-bbb:succeeded`) instead of a corrupted tangle.

---

## Appendix A — Starting checklist (avoid the first five errors)

1. **Wire a snapshot sink** or the trace bundle stays empty
   (`TaskOrchestrator(snapshot_sink=ProtocolSnapshotSink(store.snapshot))`);
2. **Register every semaphore resource before first acquire** (R16
   `KeyError` otherwise) — at minimum `task_execution` and your call-site
   resources (`llm`, `vector_search`, ...). Registration is idempotent;
   re-registering with different params only logs a warning, the first
   registration wins.
3. **Declare vocabularies up front**: `reuse_terminal_words` (recovery),
   `success_words` (DependencyGovernor) — both reject empty sets;
4. **Register every rerunnable node in `task_factory`** or recovery skips
   it with `unknown task_id`;
5. **Size timeouts to real latency**: `wait_terminal(timeout=...)` and
   `GovernedLLMClient(timeout=...)` must exceed a single call's worst
   duration.

## Appendix B — Failure-honesty quick reference

| You see | It means | Action |
|---|---|---|
| `Traceback ... <your error>` | a task failed and was governed | none — read the node's `failed` status |
| `snapshot write failed (execution unaffected)` | observation degraded (T9) | check the sink; task is fine |
| `data-rules: 0 violations, N warnings` | legally-possible-but-review states | review; warnings never fail |
| `rejected: task not found or already terminal` | pause on a ghost/finished task | safe; surface to user if needed |
| `unknown task_id` (recovery) | node missing from `task_factory` | add it to the factory |
| `UnsupportedCapabilityError` | capability not declared (T8) | inject the capability, or stop calling it |
| `KeyError` on acquire | resource not registered (R16) | `registry.register_semaphore(...)` |

## Appendix C — Self-certification (ship checklist)

Before calling a workflow done:

```bash
# 1. the bundle validates
python -c "import json,glob;from orditect.protocol.rules import run_rules; \
lines=[json.loads(x) for f in glob.glob('trace/*.ndjson') for x in open(f) if x.strip()]; \
r=run_rules(lines); print(r.summary()); assert r.ok"

# 2. your project has a smoke test (mirror the examples' meta tests)
python -m pytest tests/meta/test_example_mvp.py -v
```

Write your own smoke test the same way `tests/meta/test_example_mvp.py`
does: run the pipeline end-to-end in a subprocess, require the OK marker
plus `0 violations`.

## Appendix D — Boundaries (deliberately not in this guide)

| Topic | Where to look |
|---|---|
| PostgreSQL / MinIO / Milvus adapters (production storage) | `packages/protocol/README.md` (adapter authoring) + ROADMAP |
| Bridge implementations (LangChain / AutoGen / DeepAgent) | `docs/bridge-discipline.md` + `examples/*` bridge usage patterns |
| Production frontend (React/Vue rendering of the views) | your product layer — Chapter 7.4 gives the read endpoints |
| Lua script / Redis key internals | `packages/core/docs/lua_contract.md` |
| The 12 normative terms (T1-T12) | `packages/protocol/docs/terms.md` |
| Roadmap, commercial options, what we will NOT do | `docs/ROADMAP.md` |
| Stability commitments (frozen vs ratifying surfaces) | `docs/stability.md` |

The contribution discipline when you extend the framework itself:
public injection points only (sinks, protocols, `task_factory`, gateway
wrappers) — never monkey-patch underscore-prefixed internals, never
hand-roll a `reopen` outside `task_reopen.lua`.
