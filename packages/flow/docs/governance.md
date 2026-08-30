# Dependency Governance API Guide (v0.1.1)

`DependencyGovernor` governs **multi-parent dependency relationships** for
tasks. It is a **passive, protocol-layer API**: it registers relationships,
answers readiness, collects cancel votes, processes terminal notifications,
audits result consumption, and invalidates exemption snapshots. It never
creates tasks, never schedules execution, and never interprets DAG
semantics — those belong to the external orchestration system.

## Design contract (what this is and is not)

| Rule | Meaning |
|---|---|
| Orchestration independence | The governor never drives execution. Readiness is only *surfaced*; driving is the caller's job. |
| Hot/cold separation | Redis holds counters/sets (hot). The full dependency graph lives in an optional injected cold store (`dep_graph_store`). |
| Vocabulary neutrality (T6) | Success / terminal / ready status words are caller-declared at construction. |
| Explicit capability (T8) | Missing optional capabilities raise `UnsupportedCapabilityError`, never silently degrade. |
| Observation non-blocking (T9) | All notification paths are best-effort; failures are logged, never raised. |

| Explicit capability (T8) | Missing optional capabilities raise `UnsupportedCapabilityError`, never silently degrade. |
| Observation non-blocking (T9) | All notification paths are best-effort; failures are logged, never raised. |

Additional boundaries:

- External-vocabulary boundary: `TaskOrchestrator.cancel` / `terminate`
  coerce `TaskStatus(record["status"])` and therefore only understand the
  flow vocabulary. For tasks written with an external orchestration
  system's vocabulary, use `DependencyGovernor`'s vocabulary-parameterized
  APIs (`notify_task_terminal`, `vote_cancel`) instead.

## Construction

```python
from orditect.flow.governance import DependencyGovernor

gov = DependencyGovernor(
    storage,                              # flow TaskStorageProtocol (core TaskRedisDB)
    success_words=frozenset({"succeeded"}),  # REQUIRED, caller-declared
    terminal_words=frozenset({"succeeded", "failed", "cancelled"}),  # optional
    ready_status="pending",               # optional
    lifecycle=orchestrator.lifecycle,     # optional; vote-triggered cancel
    audit_writer=protocol_audit_writer,   # optional
    dep_graph_store=my_graph_store,       # optional cold path
)
```

| Parameter | Required | Semantics |
|---|---|---|
| `success_words` | ✅ | Non-empty set of success terminal words. The ONLY auto-vote criterion: `terminal_status not in success_words`. |
| `terminal_words` | — | Terminal words used to classify parents at registration. Defaults to the flow vocabulary. |
| `ready_status` | — | Status word a task must hold to appear in `get_ready_tasks()`. Defaults to `"pending"`. |
| `lifecycle` | — | When votes reach threshold, `lifecycle.cancel(child_id)` is invoked. None = votes recorded only. |
| `audit_writer` | — | Protocol `AuditWriter`. Failures logged, never raised. |
| `dep_graph_store` | — | Duck-typed cold store: `write_dependency(child_id, parent_id, is_primary)` / `read_graph(root_id)` / `all_edges()`. Not injected: hot path works fully; `get_dependency_graph` raises `UnsupportedCapabilityError`. |

## API reference

### register_dependency

```python
await gov.register_dependency(
    child_id: str,
    parents: list[str],
    *,
    primary_parent: str | None = None,
    exempt_resources: list[str] | None = None,
) -> None
```

Called by the external orchestration system **after creating the child
task**. Parent classification:

| Parent status | remaining_deps | active_children | cancel_votes |
|---|---|---|---|
| non-terminal | +1 | SADD | — |
| terminal & success | — | — | — |
| terminal & not success | — | — | SADD (already-cast vote) |

Notes:
- **Idempotent retry** of the same `(child_id, parents)` is safe (full rewrite).
- `primary_parent` defaults to `parents[0]` and must be in `parents`.
- `exempt_resources` snapshot: explicit list (cap 10) or inherited along the primary-parent chain.
- Raises `TaskNotFoundError` (child or parent missing), `ValueError` (cycle / cap exceeded / primary not in parents).

### get_ready_tasks

```python
await gov.get_ready_tasks() -> list[str]
```

Returns task_ids with `remaining_deps <= 0` and `status == ready_status`.
Read-only; schedules nothing. SCAN-based: intended for <=10k tasks; poll no
faster than ~100ms.

### vote_cancel

```python
await gov.vote_cancel(parent_id: str, child_id: str) -> bool
```

Casts a cancel vote. Returns True when this vote reached the threshold
(`len(depends_on)`) and cancellation was triggered. Atomic (SADD+SCARD in
one MULTI/EXEC): exactly one concurrent voter triggers. Returns False for
missing/terminal child or unregistered parent.

### notify_task_terminal

```python
await gov.notify_task_terminal(task_id: str, terminal_status: str) -> None
```

**Unified entry called after ANY task reaches a terminal state.** Two directions:

- **As a parent**: for each active child, DECR `remaining_deps` (success
  never auto-votes); if `terminal_status not in success_words`, SADD an
  automatic cancel vote (hang prevention).
- **As a child**: SREM itself from every parent's `active_children`; clear
  its own `cancel_votes`.

Best-effort (T9): failures logged, never raised. **Never invoked by the
built-in executor** — wiring it at the task-closure point is the
composition root's / bridge layer's responsibility.

### get_dependency_graph

```python
await gov.get_dependency_graph(root_id: str) -> dict
```

Cold-path query over `dep_graph_store`. Raises `UnsupportedCapabilityError`
when not injected.

### result_consumed

```python
await gov.result_consumed(task_id: str, consumer_id: str) -> None
```

Dedup audit by `(task_id, consumer_id)`: first consumption writes one
`result_consumed` audit event; repeats are silent. Internal framework
`get_task()` calls never trigger this.

### invalidate_exempt_snapshot

```python
await gov.invalidate_exempt_snapshot(task_id: str) -> None
```

Resets the exemption snapshot to None (falls back to the live ancestor
walk). Call after `reopen_task` and before re-execution. `RecoveryService`'s
rerun path invokes this automatically when a governor is injected.
Holder-liveness boundary (known, tracked separately): both the live
ancestor walk and the exemption snapshot do NOT verify that the resource
holder is still alive — an ancestor's `resource` field persists in its
record after the ancestor terminates. A child started later may therefore
be exempted against a slot that is no longer held. This is a documented
semantic boundary of the v0.1.1 exemption mechanism, not a v0.1.5 fix
target.

## Call sequence (three-parent child)

```
External Orchestrator            DependencyGovernor              Redis
        |                              |                           |
        |-- create tasks p1,p2,p3,c -->|                           |
        |-- register_dependency ------>|-- classify parents        |
        |   (c, [p1,p2,p3])            |-- SADD active_children    |
        |                              |-- SET remaining_deps=3    |
        |                              |-- write hot record fields |
        |                              |                           |
        |-- p1 reaches terminal ------>|                           |
        |-- notify_task_terminal ----->|-- DECR remaining_deps(c)  |
        |   (p1, "succeeded")          |   (=2; success: no vote)  |
        |                              |                           |
        |-- p2 fails ----------------->|                           |
        |-- notify_task_terminal ----->|-- DECR (=1)               |
        |   (p2, "failed")             |-- SADD cancel_votes(c,p2) |
        |                              |                           |
        |-- p3 succeeds -------------->|                           |
        |-- notify_task_terminal ----->|-- DECR (=0)               |
        |   (p3, "succeeded")          |                           |
        |                              |                           |
        |-- get_ready_tasks ---------->|-=["c"] (remaining<=0 &    |
        |                              |        status==pending)   |
        |-- schedule c itself -------->|  (orchestrator's job)     |
```

## Bridge wiring pattern (external orchestration framework)

A bridge (`orditect-bridge-*`) wires an external framework's task lifecycle
to the governor. The pattern is always the same three hook points:

```python
class MyFrameworkBridge:
    """Reference wiring for an external orchestration framework."""

    def __init__(self, orchestrator, dependency_governor):
        self._orch = orchestrator
        self._gov = dependency_governor

    # 1. after the framework creates a child task with its parents known
    async def on_child_created(self, child_id: str, parent_ids: list[str]):
        await self._gov.register_dependency(child_id, parent_ids)

    # 2. after ANY task the framework runs reaches a terminal state
    async def on_task_terminal(self, task_id: str, terminal_status: str):
        await self._gov.notify_task_terminal(task_id, terminal_status)

    # 3. the framework's scheduler asks what is ready
    async def schedulable(self) -> list[str]:
        return await self._gov.get_ready_tasks()
```

Discipline for bridge authors:
- **Call `notify_task_terminal` for every terminal task**, not only parents
  — the governor needs the as-child direction for cleanup.
- **Never** call `notify_task_terminal` from inside `orditect-flow`'s own
  executor; the executor is vocabulary-neutral and does not know your
  orchestration loop.
- If the external framework reopens tasks itself (bypassing
  `RecoveryService`), it must call `invalidate_exempt_snapshot` after reopen.
- Without a cold `dep_graph_store`, do not call `get_dependency_graph`
  (it raises `UnsupportedCapabilityError` by design).

## Fault tolerance summary

| Scenario | Behavior |
|---|---|
| Cold graph write fails | error log + `dep_index_write_failed` audit; registration still succeeds |
| DECR on missing counter | counter becomes -1; tolerated (readiness is a <=0 threshold), warning logged |
| Duplicate notify | counter goes negative; `get_ready_tasks` still lists the task once |
| Caller never notifies | counter never updates; task stays pending — **caller's responsibility, no compensation** |
| Parent success terminal | no automatic vote (pinned) |
| Parent abnormal terminal | automatic vote (hang prevention) |
| Redis restart loses counters | `rebuild_dep_counters(storage, dep_graph_store)` from the cold store; without cold store, not rebuildable |
| Concurrent votes | exactly one triggers cancellation (MULTI/EXEC atomicity) |

## Offline tools

```python
from orditect.flow.governance import scan_dependency_cycles, rebuild_dep_counters

# line-2 cycle detection (register-time DFS is line 1); run as cron/ops
cycles = await scan_dependency_cycles(dep_graph_store)

# admin recovery after Redis restart
stats = await rebuild_dep_counters(storage, dep_graph_store)
# -> {"rebuilt": int, "skipped": int, "errors": int}
```