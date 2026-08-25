# orditect-stream

**Streaming rich-media output plane for the Orditect ecosystem**

[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)

`orditect-stream` is the output plane of the Orditect ecosystem: standard SSE
protocol, multi-stream mux, rich-media placeholders, disconnect strategies —
turning LLM streaming output into a governable, observable, recoverable
production capability.

## Core Capabilities

- 🌊 **Standard SSE protocol**: stream.delta / enrich.* / stage.end /
  stream.manifest (golden-test frozen schema; business extension only via
  `data.ext`)
- 🔀 **Multi-stream mux**: max_id concurrent sub-streams per request,
  per-stream monotonic seq
- 🖼️ **Rich-media placeholders**: in-stream image markers → placeholder →
  settle-window backfill / manifest delegation
- 🧠 **thinking three modes**: inline / separate / suppress (reasoning-model
  chain-of-thought handling)
- 🔌 **Disconnect strategies**: cancel / grace (grace-period reconnect) /
  continue
- ❌ **Dual-mode cancel**: `cancel()` graceful (stops output, sem held until
  LLM ends) / `cancel(force=True)` forced (coroutine cancelled)
- 🛡️ **Resource governance**: stage-level resource injection
  (default_stream_llm special holding semantics)

## Installation

```bash
pip install orditect-stream[fastapi]
# depends on orditect-core>=0.1, orditect-flow>=0.1, orditect-protocol>=0.1
```

## Quick Start
```python
from orditect.stream import (
    DEFAULT_CONFIG, EnrichMode, EventType, MockVectorEnricher,
    SourceChunk, SourceRequest, StageConfig, SourceType, StreamRunner,
)
from orditect.stream.store import MemoryResultStore

class MyLLMSource:
    async def stream(self, request: SourceRequest, cancel_token=None):
        yield SourceChunk(text="正文![img]完")
        yield SourceChunk(finish=True)

runner = StreamRunner(
    stages=[StageConfig(name="main", source_type=SourceType.LLM, source=MyLLMSource())],
    enricher=MockVectorEnricher(),
    store=MemoryResultStore(),
    config=DEFAULT_CONFIG.merge(enrich_mode=EnrichMode.LOCAL),
)

async for env, et in runner.run():
    if et == EventType.STREAM_DELTA:
        print(env.data.get("text"))
```

### FastAPI integration
```python
from fastapi import FastAPI, Request
from orditect.stream.fastapi import create_stream_response, make_refetch_router

app = FastAPI()
store = MemoryResultStore()
app.include_router(make_refetch_router(store))

@app.get("/stream")
async def stream(request: Request):
    runner = StreamRunner(
        stages=[...], enricher=..., store=store,
        config=DEFAULT_CONFIG,
        loading_url="https://oss.example.com/loading.jpg",
    )
    return create_stream_response(runner, request)
```
### Cancel & partial content
```python
# Graceful cancel (default): stop output, LLM connection kept, sem held until LLM ends
await runner.cancel(stream_id=sid, reason="user_interrupt")

# Forced cancel: coroutine cancelled, sem released immediately
await runner.cancel(stream_id=sid, force=True)

# Partial content not lost on cancel: delivered via stream.cancelled event,
# also queryable (for business-side history saving)
partial = runner.get_partial_content(sid)
```


### Protocol-backed result store (v0.1.0)
```python
from orditect.adapter.memory import MemoryStore
from orditect.stream.store import get_protocol_store

parts = MemoryStore()
store = get_protocol_store(parts.result, parts.result)  # protocol result domain
# Production: swap in a commercial PG adapter's result part

```

## Documentation

- [Event protocol](docs/protocol.md): SSE frame format & event schema
  (golden frozen) + pause/resume semantics decision

## Testing
```bash
pytest tests/unit -v    # pure logic
pytest -m golden        # protocol snapshot
pytest tests/ -v        # full suite
```

## Related Projects

- **[orditect-core](../core)**: governance engine (rate limiting, connection pool)
- **[orditect-flow](../flow)**: orchestration plane (enrich task backend, recovery)
- **[orditect-protocol](../protocol)**: storage interaction contracts

## License

Apache-2.0
