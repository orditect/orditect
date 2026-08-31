# Orditect MVP Example

The full governance loop with **zero infrastructure** — no Redis, no API
keys, no services. A fresh clone runs it in one command.

## What it demonstrates

| Concern | Framework piece |
|---|---|
| Local storage (trace bundle) | `orditect-adapter-local` (`LocalFileStore`) |
| Bridge-style workflow | `orditect-bridge-openai` (`GovernedLLMClient`, mock endpoint) + recursive `BaseBackEndTask` |
| Workflow visualization | `orditect-adapter-ui` (`TraceBundleReader`: tree / generations / dependency graph / audit) |
| HITL pause / retry / resume | `ActionSinkAdapter` + `ActionDispatcher` (command-queue form, DD-013) |
| Data-level certification | `run_rules` over the produced trace bundle |

## Run

```bash
pip install -r requirements.txt
python run_demo.py
```

Expected narrative: the pipeline runs `collect -> analyze -> report`, the
report node fails on its first generation, an HITL retry reopens a new
generation and succeeds, then a slow node is paused (cancelled) and the
tree is resumed — succeeded nodes are reused, only the cancelled node
reruns. The trace bundle passes `run_rules` with zero violations.

## Key design points

1. **Visualization switch**: `TaskOrchestrator(snapshot_sink=ProtocolSnapshotSink(...))`.
   Without it the executor uses the NullSink and no snapshots are written.
2. **Hot/cold separation**: the demo's in-memory doubles
   (`InMemoryTaskStorage` / `InMemoryGovernor` / `InMemoryQuota`) mirror
   the production Redis trio (`TaskRedisDB` / `AsyncLeaseSemaphore` /
   `AdmissionQuotaRedisDB`). Swap them, keep every line of business code.
3. **HITL is queue-shaped**: UI/MCP/agents never touch the hot path.
   Actions are enqueued commands, executed asynchronously by the
   dispatcher; action records double as audit events (`event_id == action_id`).
4. **call_id dual-habitat idempotency**: bridge calls pass explicit
   `call_id`s; retries with the same key dedup at both the quota layer
   and the audit layer.
5. **Self-certifying output**: `run_rules` validates the trace bundle
   against the data rules (T3 terminal drift, T4 idempotency, T11
   execution_id, T7 clock discipline).

## Production swap (real Redis + real LLM)

Replace the three hot-path doubles:

```python
import redis.asyncio as aioredis
from orditect.core import get_registry, AdmissionQuotaRedisDB
from orditect.flow.storage.factory import get_default_storage
from orditect.flow.governor.factory import TaskbaseGovernorAdapter

client = aioredis.from_url("redis://localhost:6379/0")
storage = get_default_storage(client)
await storage.connect()
registry = get_registry()
registry.register_semaphore("llm", client, limit=30, lease_time=30.0)
registry.register_semaphore("task_execution", client, limit=10)
governor = TaskbaseGovernorAdapter(registry)
quota = AdmissionQuotaRedisDB(client=client)
await quota.connect()
```

For a real LLM, drop the injected `http_client` in `make_llm` and point
`base_url` at any OpenAI-compatible endpoint with an `api_key`.