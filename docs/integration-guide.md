# Orditect Integration Guide (v0.1.3)

How to wire the three categories (core / adapters / bridges) into a working
governance loop. This guide is for teams integrating Orditect into their
own workflows or building custom adapters/bridges.

## The Three Categories

core (framework, you don't modify it)
  = protocol (shared contracts)
  ← core (redis hot path: semaphore, quota, task store)
  ← flow (orchestration: GovernedClient, Recovery, DependencyGovernor)
  ← stream (output plane: SSE protocol)

adapters (storage + UI, you implement or reuse)
  = storage adapters: localfile / redis-cold / pg / minio / s3 / milvus
  = UI adapters: consumer read (trace bundle) + action sink (HITL/MCP/agent)

bridges (producers, you implement or reuse)
  = framework bridges: langgraph / langchain / autogen / deepagents
  = endpoint bridges: openai-compatible endpoints
  = capability bridges: tools / memory / skills
  = legacy bridges: traditional non-AI workflows

## Integration Path

### 1. Storage adapter (what you implement or reuse)

Implement the five protocol domains (or reuse an existing adapter):

```python
from orditect.adapter.local import LocalFileStore  # zero-infrastructure

store = LocalFileStore("/var/lib/myapp/trace")
# store.content / store.audit / store.result / store.snapshot / store.dependency
```

Or implement your own (PostgreSQL, S3, etc.) by satisfying the protocol
contracts and passing the conformance suite:
```python
from orditect.protocol.conformance import run_conformance

def test_my_adapter():
    report = run_conformance(my_store.snapshot, profile="full")
    assert report.failed == 0, report.summary()
```
### 2. Bridge (what you implement or reuse)

For LLM calls (endpoint bridge):
```python
from orditect.bridge.openai import GovernedLLMClient

llm = GovernedLLMClient(
    "https://api.openai.com",
    api_key="sk-...",
    governor=governor,  # from orditect-flow
    resource="llm",
    budget=ledger,      # BudgetLedger from orditect-flow
    audit_writer=store.audit,
    content_writer=store.content,
    model="gpt-4o",
    task_id="my-task",
)

result = await llm.chat(messages=[...])
```
For orchestration frameworks (framework bridge): translate framework
concepts (nodes, edges, executions) into Orditect models (TaskSnapshot,
DependencyEdge, AuditEvent) at the bridge edge, write via the protocol
sink, and certify under the producer profile.

### 3. UI adapter (what you implement or reuse)

Read trace bundles and drive actions:
```python
from orditect.adapter.ui import TraceBundleReader, ActionSinkAdapter, MemoryActionQueue
from orditect.flow.actions import ActionDispatcher

# consumer read
reader = TraceBundleReader("/var/lib/myapp/trace")
tree = await reader.snapshot.get_tree("root-task")

# action sink (command-queue form)
queue = MemoryActionQueue()  # or Redis-backed queue in production
sink = ActionSinkAdapter(queue, audit_writer=store.audit)
dispatcher = ActionDispatcher(queue, orchestrator, recovery)

await dispatcher.start()
receipt = await sink.pause_node("task-123", actor="user-456")
```


## The Governance Loop (what happens when it all works)

1. **Bridge** makes a governed LLM call → semaphore acquired, budget checked.
2. **Core** enforces concurrency limits and tracks usage.
3. **Bridge** writes audit events and pointer-ized content to **storage adapter**.
4. **Storage adapter** persists snapshots, audits, edges, manifests, blobs.
5. **UI adapter** reads the trace bundle for observability.
6. **UI adapter** (or HITL/MCP/agent) writes action commands to the queue.
7. **Flow dispatcher** consumes actions and executes pause/retry/resume.
8. **Flow** updates task state in the hot path (Redis), writes new snapshots.

## Certification Checklist

- [ ] Storage adapter passes full profile (five domains, sink/query paired)
- [ ] Bridge passes producer profile (sink half-domains as declared)
- [ ] UI adapter passes consumer profile (query half-domains + seed hook)
- [ ] UI adapter action sink passes action profile (pause/retry/resume work)
- [ ] Trace bundle readable by run_rules with zero violations
- [ ] Swap tests: bridge ↔ adapter, adapter ↔ UI, all interchangeable

## Boundary Discipline

- **Never** import business frameworks (langchain, openai, etc.) into
  core/flow/stream/protocol — bridges translate at the edge.
- **Never** put business vocabulary (model names, event types) into
  protocol contracts — all status/type fields are opaque strings.
- **Never** touch the hot path directly from UI/MCP/agent — actions go
  through the queue; the dispatcher executes asynchronously.
