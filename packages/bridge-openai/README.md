# orditect-bridge-openai

OpenAI-compatible **endpoint bridge** for the Orditect ecosystem (bridge
reference implementation, producer tier).

## Purpose

- Reference bridge: the first external producer passing the protocol
  conformance suite under the **producer** profile.
- Governed LLM calls: semaphore, budget, audit, and content pointer-ization
  wrapped around any OpenAI-compatible endpoint (OpenAI, Azure, vLLM,
  Ollama, LM Studio, ...).
- Two call forms with one client: non-streaming `chat()` and streaming
  `stream()` (implements `LLMSourceProtocol` for orditect-stream).

## Boundary

This is a **bridge**, not a framework package: OpenAI-shaped vocabulary
(model / messages / usage / finish_reason) lives here and never flows back
into core / flow / stream / protocol.

## Usage

```python
from orditect.bridge.openai import GovernedLLMClient
from orditect.adapter.memory import MemoryStore

parts = MemoryStore()
llm = GovernedLLMClient(
    "https://api.openai.com", api_key="sk-...",
    governor=governor, resource="llm",
    budget=ledger,
    audit_writer=parts.audit,
    content_writer=parts.content,
    model="gpt-4o",
    task_id="my-task",
)

result = await llm.chat(messages=[{"role": "user", "content": "hi"}])

# streaming (orditect-stream compatible)
async for chunk in llm.stream(messages=[...]):
    ...
```

## Streaming termination discipline

`stream()` classifies every stream's ending and makes the classification
auditable:

| Ending | Detection | Behavior |
|---|---|---|
| completed | `[DONE]` sentinel or a `finish_reason` | protocol finish chunk appended; audit carries `termination=completed` + `finish_reason` |
| truncated | connection closed without either marker | raises `StreamTruncatedError` (audit carries `termination=truncated` + chunk count) |
| empty | 200 OK with zero frames | raises `StreamEmptyError` (endpoint incompatibility, e.g. `stream_options`) |

A `finish_reason="length"` (max_tokens reached) is a protocol-completed
ending, NOT an exception — it is recorded in the audit payload so a
silently shortened body stays visible downstream.

Token usage is captured from the stream's tail chunk (which carries an
empty `choices` list), so `cost_fn` receives real token figures for
streams instead of `None`.

## Testing

```bash
# unit + mock-SSE termination matrix (no network, no credentials)
python -m pytest tests -q -m "not live"

# live endpoint tests (skipped without .env)
# .env keys: LLM_BASE_URL / LLM_API_KEY / LLM_PUBLISH_MODEL
python -m pytest tests/test_stream_live.py -v -m live
```

### Live stream probe

`tests/probe_live_stream.py` drives a real endpoint and prints every
chunk plus the resulting audit payload:

```bash
# full stream: complete body, finish chunk, termination=completed,
# real usage/cost in the audit payload
python -m tests.probe_live_stream

# max_tokens truncation: finish_reason=length (completed, auditable)
python -m tests.probe_live_stream --max-tokens 24 "long essay prompt"

# cancel beat: output stops after N chunks while consumption drains to
# the real terminal point; the drain window and the full token bill
# (cancel does NOT save tokens) show up in the summary
python -m tests.probe_live_stream --cancel-after 30
```

Reference output (dashscope, qwen thinking model):

```text
# full stream
finish chunk:     True
termination     : completed
finish_reason   : stop
usage           : total_tokens=2582   cost_units: 2582

# cancel after chunk #30
cancelled at:     1.0s
drain window:     27.8s (consumption after cancel until stream end)
finish chunk:     True
termination     : completed
usage           : total_tokens=3154   cost_units: 3154
```

The cancel comparison is the design statement in numbers: interrupting
the consumer does not end the upstream call — the semaphore stays held
until the API stream truly finishes, and the bill covers every token
generated, drained or not.