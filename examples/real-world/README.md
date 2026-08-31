# Orditect Real-World Example

The SAME business code as `examples/mvp`, running against the **production
hot path (Redis)** and a **real OpenAI-compatible LLM**. This is the living
proof of Orditect's hot/cold replaceability: swap the infrastructure, keep
every line of business code.

## Prereqs

1. **Redis** reachable at `REDIS_URL` (default `redis://localhost:6379/0`).
2. **An OpenAI-compatible endpoint**, e.g.:
   ```bash
   ollama serve
   ollama pull qwen2.5:7b
   ```
   (vLLM / LM Studio / OpenAI all work — anything speaking
   `POST {BASE_URL}/chat/completions`.)
3. Configure:
   ```bash
   cp .env.example .env   # then edit values
   ```

## Run

```bash
pip install -r requirements.txt
python run_demo.py
```

## What differs from the MVP (and what does not)

| Aspect | MVP | This demo |
|---|---|---|
| Task store | `InMemoryTaskStorage` | `TaskRedisDB` (Redis + Lua) |
| Semaphore | `InMemoryGovernor` | `AsyncLeaseSemaphore` (via `LimiterRegistry`) |
| Quota | `InMemoryQuota` | `AdmissionQuotaRedisDB` (ZSET lease) |
| LLM endpoint | `httpx.MockTransport` | real `LLM_BASE_URL` |
| **Business code (`tasks.py`, workflow, HITL calls)** | — | **identical** |

## Notes

- The Redis keys live under the default prefixes (`task:*`, `task_status:*`,
  `{ftb}:semaphore:*`, `admission:*`). Point `REDIS_URL` at a dedicated
  logical DB (e.g. `/14`) if you want isolation from other data.
- `LLM_SEM_LIMIT` caps concurrent LLM calls; the executor's own
  `task_execution` semaphore is registered with limit 10.
- Audit events now carry the real endpoint's `usage` (token counts) and
  `elapsed_ms`, so the budget deducts real tokens instead of the mock's
  fixed 30.