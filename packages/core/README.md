# orditect-core

**Async task governance engine for the Orditect ecosystem**

[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)

`orditect-core` is the governance engine of the Orditect ecosystem: Redis task
store (state machine hosting), distributed rate limiting (lease semaphore +
reservation token bucket), unified connection pool management.

## Core Capabilities

### Task Store (TaskRedisDB)

- **State machine hosting**: `terminal_statuses` / `transitions` instance-level
  injection — upper frameworks declare their own state machine semantics,
  Lua terminal protection and Python transfer validation bound to the same source
- **Lua atomic operations**: task updates (merge + status index maintenance)
  completed in a single script atomically
- **Lineage index** (B2): `parent_task_id` registration + `list_children()`
  query (data foundation for cascade cancel / observation tree)
- **Idempotent initialization** (B3): `initialize_task(if_not_exists=True)`
  prevents retry from resetting running task state
- **Index and primary record share the same expiry**: same TTL, naturally no
  ghost members (design contract, T9 pinned)

### Distributed Rate Limiting

- **AsyncLeaseSemaphore**: ZSET lease semaphore + watchdog renewal
  (long task holding without expiry, crash auto-recovery, capacity no
  doubling no leakage)
- **AsyncTokenBucket**: reservation token bucket (server-side clock,
  zero Redis connection occupancy during wait, rejected requests don't burn
  future quota)
- **@limited decorator**: declarative resource governance (wait/reject dual mode)
- **LimiterRegistry**: global resource registry + status query interface
  (limit/usage/available/utilization five-field unified format)

### Connection Pool Management

- **RedisPoolManager**: global singleton, unified register/monitor/close
- **Dependency injection**: `RedisDB(client=...)` shares connection pool,
  `RedisDB(redis_url=...)` self-managed (backward compatible)

## Installation

```bash
pip install orditect-core
```

## Quick Start

### Task Store (standalone, default vocabulary)
```python
from orditect.core import TaskRedisDB

db = TaskRedisDB("redis://localhost:6379/0")
await db.connect()

await db.initialize_task("task_001")
await db.update_task("task_001", {"status": "in_progress"})
task = await db.get_task("task_001")
```

### State Machine Hosting (custom vocabulary)
```python
from orditect.core import TaskRedisDB

# Upper framework declares its own state machine (e.g. flow's vocabulary)
db = TaskRedisDB(
    "redis://localhost:6379/0",
    terminal_statuses=("succeeded", "failed", "cancelled"),
    transitions={
        "": {"pending", "queued"},
        "pending": {"queued", "cancelled"},
        "queued": {"running", "cancelled"},
        "running": {"succeeded", "failed", "cancelled"},
        "succeeded": set(), "failed": set(), "cancelled": set(),
    },
)
await db.connect()

# succeeded is a declared terminal state: any overwrite is atomically rejected by Lua

```
### Lineage & Idempotency (recursive composition)

```python
# Parent task submits child task: lineage registration + idempotency
await db.initialize_task(
    "child_task",
    parent_task_id="parent_task",
    if_not_exists=True,  # skip if exists (returns False), don't reset state
)

children = await db.list_children("parent_task")  # ["child_task", ...]

```

### Distributed Semaphore
```python
from orditect.core import AsyncLeaseSemaphore

sem = AsyncLeaseSemaphore(redis_client, "llm", limit=30, lease_time=30.0)

async with sem.hold():  # hold() produces independent context per call (concurrency-safe)
    await long_running_llm_call()  # watchdog auto-renews, holding 5 hours no problem

```

### Declarative Decorator
```python

from orditect.core import limited, get_registry

registry = get_registry()
registry.register_semaphore("llm", redis_client, limit=30)

@limited(resource="llm", mode="wait", timeout=5.0)
async def call_llm(prompt: str):
    return await llm_client.chat(prompt)

@limited(resource="gpu_pool", mode="reject")  # reject immediately (app maps 429)
async def submit_task(data: dict):
    return await heavy_computation(data)
```

### Connection Pool Management
```python
from orditect.core import get_pool_manager, TaskRedisDB

pool_manager = get_pool_manager()
redis_client = pool_manager.register_pool(
    "default", "redis://localhost:6379/0", max_connections=200,
)

# All modules share connection pool (dependency injection)
task_db = TaskRedisDB(client=redis_client)

# Monitoring
stats = await pool_manager.get_pool_stats("default")
# {"name": "default", "max_connections": 200, "in_use": 15, ...}

# On application shutdown
await pool_manager.close_all()
```

## Important Changes

- **Bare `async with sem:` removed** (breaking): semaphore instances shared
  across coroutines, context state on instance attributes causes concurrent
  overwrites (mutual exclusion failure + slot permanent leakage). Use
  `async with sem.hold():` — each call produces independent context object,
  naturally concurrency-safe.
- **update_task without expiry preserves remaining expiry** (behavior change):
  no longer resets to default_expire_time; explicit expiry advances the lease.
- **task_update.lua ARGV spec change**: new ARGV[7] (fallback TTL for preserve
  mode), see `docs/lua_contract.md`.

## Documentation

- [Lua Script Contract](docs/lua_contract.md): 8 scripts' ARGV specs frozen
- [Design Decisions](docs/design_decisions.md): fail-open/close strategy,
  Cluster key conventions, quota renewal, etc.

## Testing

```python
pytest -m unit          # pure logic
pytest -m pinning       # behavior pinning (requires Redis, default db15)
pytest -m integration   # acceptance tests
python -m pytest tests/ -v -m "not chaos"   # full suite
```


## Related Projects

- **[orditect-protocol](../protocol)**: storage interaction contracts
- **[orditect-flow](../flow)**: orchestration (recursive composite task governance, hard dependency on this framework)
- **[orditect-stream](../stream)**: output plane (SSE streaming output)

## License

Apache-2.0

---