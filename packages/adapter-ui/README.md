# orditect-adapter-ui

UI adapter **reference implementation** for the Orditect ecosystem.

## Purpose

- Consumer-side read: parse a **trace bundle** (ndjson + JSON produced by
  `orditect-adapter-local` or any conformant adapter) without importing
  orditect-core / orditect-flow internals.
- Action sink: a protocolized channel for **HITL / MCP / agent** to drive
  pause / retry / resume on a task tree, delegated to orditect-flow's
  public operation surface.

This is a **reference implementation**, not a frontend product. It exists
to prove the UI adapter protocol (consumer read + action sink) is
implementable.

## Two halves

| Half | Surface | Certification |
|---|---|---|
| consumer (read) | snapshot/dependency/audit/result/content queries over a trace bundle directory | consumer profile |
| action (write) | `pause_node` / `retry_node` / `retry_scope` / `resume_tree` | action profile |

## Usage

```python
from orditect.adapter.ui import TraceBundleReader, ActionSinkAdapter

reader = TraceBundleReader("/path/to/trace-bundle")
tree = await reader.snapshot.get_tree("root-task")
events = await reader.audit.query(task_id="child-1")

sink = ActionSinkAdapter(orchestrator, recovery_service)
await sink.pause_node("node-3")
await sink.retry_scope("root-task", scope={"node-a", "node-b"})
await sink.resume_tree("root-task")