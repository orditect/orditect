# Flow Recovery Plane (F2–F4)

The recovery plane provides resume / rerun over the recursive task tree.
It is the orchestration-side carrier of breakpoint-resume, mid-point
custom replay, and time-travel, built on three layers:

```code
orditect-core    reopen_task (hot-path new-generation opening, T3-safe)
orditect-protocol snapshot domain (warm-path tree/version queries)
orditect-flow    snapshot sink + reuse short-circuit + RecoveryService
```


## Three cooperating primitives

| Primitive | Layer | Role |
|---|---|---|
| `snapshot_sink` (F2) | executor | writes execution snapshots at lifecycle points (running / terminal) |
| `snapshot_query` + `reuse_terminal_words` (F3) | executor | short-circuits re-execution of already-succeeded nodes |
| `RecoveryService` (F4) | orchestration | resume / rerun over the task tree |

## Per-node decision algorithm

For each node in the subtree (latest generation):
```python
if latest snapshot status in reuse_terminal_words
   AND core hot record carries a result:
       -> REUSE   (no re-execution; result reused from hot record)
else:
       -> RERUN   (core reopen_task opens a new generation,
                   executor.execute drives re-execution)
```


`reuse_terminal_words` is caller-declared (vocabulary neutrality, T6) — the
framework embeds no success words.

## Execution dispatch (design decision)

Rerun **bypasses `orchestrator.submit`** and calls `executor.execute`
directly. Reason: `reopen_task` has already reset the node to its initial
state for the new generation; re-running submit's `initialize_task` would
double-register and conflict with the reopened state. Reopen-then-execute
is the correct order — reopen resets, execute drives running落账 → F3 reuse
query → execution (a reopened node has no success snapshot in its new
generation, so it executes).

## task_factory (boundary discipline)

The framework does not know how to reconstruct a task from a task_id — that
is business semantics. The caller injects `task_factory`:

```python
async def task_factory(task_id: str) -> BaseBackEndTask:
    spec = await my_business_registry.lookup(task_id)
    return spec.build(storage, governor)

svc = RecoveryService(
    storage=task_db,
    snapshot_reader=pg_snapshot_reader,   # protocol SnapshotReader (adapter)
    executor=orchestrator.executor,
    reuse_terminal_words=frozenset({"succeeded"}),
    task_factory=task_factory,
)
```

A task registry (task_id -> task class) can be built on top of this by the
business / commercial resume product; the framework deliberately provides
only the factory injection point.

## resume vs rerun

- `resume(root_task_id)`: recover the whole subtree — reuse succeeded nodes,
  rerun the rest. Breakpoint-resume is the common case.
- `rerun(root_task_id, scope={...})`: force-rerun an explicit node set;
  nodes outside scope follow the normal decide(). Mid-point custom replay.

## execution_id alignment (T11)

- core hot record: `initialize_task` assigns the first generation (C3.5),
  `reopen_task` advances it.
- flow: snapshot sink reads the current execution_id into every snapshot.
- protocol snapshot domain: versions are listed by
  (task_id, step, execution_id).

All three projections share the same value and semantics; any divergence
corrupts time-travel.

## Data retention

Redis hot records keep only the latest generation (+ previous_execution_ids
trace, capped at 50). Full history lives in the protocol snapshot domain
(written by the snapshot sink), keeping Redis free of history burden.



## Pause semantics (F5 / v0.2.0 decision — no standalone suspend mechanism)

### Why there is no suspend primitive

The governance objects of this framework are AI-workflow resources
(LLM / agent calls), not traditional CPU tasks. An LLM request, once
dispatched, cannot be "paused" mid-generation — the semaphore slot is spent
waiting for the response, and there is no interruptible in-flight execution
state to freeze. Consequently, "pause" has no independent mechanism here:

> **Pause = cancel/terminate at a node boundary + resume/rerun to continue.**

The existing three primitives fully cover pause semantics:
- `cancel(task_id)` / `terminate(task_id)` — end the current node
  (sem released per its holding semantics; the LLM call completes or is
  abandoned),
- `resume(root)` / `rerun(root, scope)` — continue from that node.

No new state word, no suspend flag, no event-wait machinery is introduced —
those belong to traditional CPU-task governance and would be over-design
here.

### Pause/resume playbook (breakpoint patterns)

**Pause at a node:**
```python
await orchestrator.terminate(node_id)   # stop at the node boundary
```
**Resume from that node to the end of the tree:**
```python
svc = RecoveryService(storage, snapshot_reader, orchestrator.executor,
                      reuse_terminal_words=frozenset({"succeeded"}),
                      task_factory=my_factory)
plan = await svc.resume(root_task_id)   # succeeded nodes reused, rest rerun
```
**Re-run between two nodes (interval replay, for inter-node testing):**
```python
plan = await svc.rerun(root_task_id, scope={node_a, ..., node_b})
```

All intermediate and final results are versioned via the snapshot domain
(task_id, step, execution_id), so replaying any interval preserves history
for comparison.

### Cascade

suspend-as-cancel and resume both follow the lineage (D5): pausing a parent
cascades to the subtree; resuming walks the subtree with per-node
reuse/rerun decisions.

### Streaming side (D6)

On pause the SSE stream ends normally with a manifest (the
"stream.end is the only terminal signal" discipline holds); resume opens a
new stream under a new execution generation. The stream layer needs no
pause-specific machinery.


## Boundary discipline & adapter guidance (D0–D6 conclusion)

### What the framework layer does NOT do

The open framework (core / flow / stream / protocol) is responsible only for:
- semaphore governance over AI-workflow resources (LLM / agent calls),
- node state recording (Redis hot records hold **pointers**, never content),
- recovery **mechanisms** (reopen / resume / rerun),
- lineage (parent_task_id) and snapshot/audit/result contracts.

It deliberately does **not**:
- fetch or pass content bodies — a `task_factory(task_id)` receives only the
  task_id; retrieving an upstream node's output body (via its pointer) is the
  caller's own responsibility, using a storage adapter,
- provide a "get upstream output" entry point — no RebuildContext, no
  content-fetch helper in the framework,
- interpret any business vocabulary or modality.

**Reason**: content bodies live in external stores (PG / MinIO / Milvus / S3),
written asynchronously (never blocking task duration). Knowing where content
lives and how to fetch it is exactly what a storage adapter implements — that
is adapter/business territory, not framework territory.

### Adapter ecosystem guidance

Open-source reference adapters (for community study, all run the protocol
conformance kit):

| Adapter | Backend | Community value |
|---|---|---|
| `orditect-adapter-memory` | in-memory | zero-dependency conformance reference |
| local file storage adapter *(planned)* | local JSON files | zero-dependency persistence; run the recovery plane without any infra |
| redis file storage adapter *(planned)* | Redis content store | simplest single-medium form (governance + content in one store); high teaching value |

Commercial adapters (closed-source, built on orditect-protocol):
production-grade / multi-modal / large-scale backends (PostgreSQL, MinIO,
Milvus, ...). Passing the conformance kit is the only compatibility
certification, and the second real backend passing it is the protocol 1.0
freeze criterion.

### Building a resume/replay product on top

A commercial resume/visualization product should:
1. implement (or reuse) a storage adapter for the snapshot + content domains,
2. inject a `task_factory` that reconstructs tasks from task_id and fetches
   any needed upstream output **itself** via its content adapter,
3. consume tree/version queries from the protocol snapshot domain for
   DAG rendering and interval selection.

The framework supplies the mechanisms; the product supplies the semantics.