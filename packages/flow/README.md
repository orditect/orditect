# orditect-flow

**Async task orchestration and lifecycle management for the Orditect ecosystem — recursive composite task governance**

[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)

`orditect-flow` is the orchestration plane of the Orditect ecosystem. Its core
capability is **recursive composition**: every task node, at any depth, can be
independently governed — concurrency, state, cancellation, resources — with
governance delegated to any layer of the call stack.

## Core Capabilities

### Recursive Composition

Any `BaseBackEndTask` can submit child tasks inside its own `execute()`, and
children can submit grandchildren — depth is limited only by business logic:

- **Automatic lineage registration**: nested `submit()` injects
  `parent_task_id` via contextvar, zero boilerplate
- **Cascade cancellation**: `cancel()` / `terminate()` recurse along the
  lineage, no orphan tasks
- **Resource lineage exemption**: a child sharing an ancestor's resource is
  exempted (ancestor's slot covers the whole subtree), no self-deadlock
- **Idempotent submit**: `submit(if_not_exists=True)` prevents parent retry
  from re-submitting children

### Task Governance

- FastAPI-native: no extra worker process, runs inside the app
- Full state machine: `pending → queued → running → succeeded/failed/cancelled`
- Dual-layer governance: task-level `resource_type` + call-site `GovernedClient`
- Dual-mode cancellation: `cancel()` graceful mark / `terminate()` force-kill
- Retry: exponential backoff, dead letter queue (DLQ)
- Workflow orchestration: DAG dependencies + Saga rollback
- Callbacks: Webhook, WebSocket, composite
- Progress tracking

### Recovery Plane (v0.1.0)

Breakpoint-resume and mid-point custom replay over the recursive task tree:

```python
from orditect.flow import RecoveryService

svc = RecoveryService(
    storage=task_db,                    # orditect-core TaskRedisDB
    snapshot_reader=pg_snapshot_reader, # protocol SnapshotReader (adapter)
    executor=orchestrator.executor,
    reuse_terminal_words=frozenset({"succeeded"}),
    task_factory=my_task_factory,       # caller-injected reconstruction
)

# Breakpoint-resume: reuse succeeded nodes, rerun the rest
plan = await svc.resume(root_task_id)

# Mid-point replay: force-rerun an explicit node set
plan = await svc.rerun(root_task_id, scope={"node_a", "node_b"})
```
Snapshot persistence is opt-in (`snapshot_sink` on the orchestrator) — zero
cost by default. See [docs/recovery.md](docs/recovery.md).

### Governed Calls & Actions (v0.1.3)

Standard form for governed resource calls and protocolized action channel:

```python
from orditect.flow import GovernedCallClient, ActionDispatcher

# Governed call (semaphore + budget + audit + content)
client = GovernedCallClient(
    governor, "llm", handler=call_llm,
    budget=ledger, audit_writer=audit, task_id=task_id,
)
result = await client.call(call_id="c-1")

# Action sink (command-queue form for HITL/MCP/agent)
dispatcher = ActionDispatcher(queue, orchestrator, recovery)
await dispatcher.start()
```
See `docs/integration-guide.md` for the three-category integration guide.


## Installation

```bash
pip install orditect-flow
# depends on orditect-core>=0.1, orditect-protocol>=0.1
```

## Quick Start

```python

from orditect.flow import BaseBackEndTask, TaskOrchestrator
from orditect.flow.storage.factory import get_default_storage
from orditect.flow.governor.factory import get_default_governor

class MyTask(BaseBackEndTask):
    async def execute(self, task_id: str, **kwargs):
        return await process(kwargs["data"])

storage = get_default_storage(redis_client)
await storage.connect()
governor = get_default_governor()
orchestrator = TaskOrchestrator(storage, governor)

task = MyTask(storage, governor)
task_id = await orchestrator.submit(task, data={"key": "value"})
status = await orchestrator.get_status(task_id)
```

## Architecture

```jsunicoderegexp
TaskOrchestrator  (submit / cancel / terminate / cascade / recovery)
       |
TaskExecutor      (resource exemption / timeout sentinel / shielded release /
                   snapshot sink / result reuse short-circuit)
       |
RecoveryService   (resume / rerun — per-node reuse-or-rerun decision)
       |
orditect-core     (governance engine: task store, semaphore, reopen primitive)
orditect-protocol (storage contracts: snapshot / content / audit domains)
```

### Multi-Parent Dependency Governance (v0.1.1)

`DependencyGovernor` is a passive, protocol-layer API for tasks with
multiple parents. It governs the dependency relationship only — task
creation, scheduling, and DAG semantics stay with the external
orchestration system:

```python
from orditect.flow.governance import DependencyGovernor

gov = DependencyGovernor(
    storage,
    success_words=frozenset({"succeeded"}),   # caller-declared (T6)
    lifecycle=orchestrator.lifecycle,          # vote-triggered cancel
    dep_graph_store=my_graph_store,            # optional cold path
)

# after creating the child task (external system's job):
await gov.register_dependency(child_id, [p1, p2, p3])

# after ANY task reaches a terminal state (external system's job):
await gov.notify_task_terminal(parent_id, "succeeded")

# poll readiness (never schedules anything):
ready = await gov.get_ready_tasks()
```
- Success never auto-votes; abnormal terminals auto-vote (hang prevention).
- Votes are atomic: exactly one concurrent voter triggers cancellation.
- Without `dep_graph_store`, the hot path works fully and
  `get_dependency_graph()` raises `UnsupportedCapabilityError` (T8).



## Documentation

- [Recovery plane](docs/recovery.md): resume / rerun design and wiring
- [Architecture](docs/architecture.md): orchestration internals

## Testing

```bash
pytest tests/unit -v    # pure logic (in-memory infra)
pytest tests/ -v        # full suite (requires Redis, default db15)
```


## Related Projects

- **[orditect-core](../core)**: governance engine (task store, rate limiting, reopen)
- **[orditect-protocol](../protocol)**: storage interaction contracts
- **[orditect-stream](../stream)**: output plane (SSE streaming)

## License

Apache-2.0