# Orditect Stream Example

Governed workflow with a **live SSE output plane**. Same Redis hot path
and real LLM as `examples/real-world`, but the analyze node streams its
LLM output to end users via the orditect-stream protocol while remaining
a flow-governed node.

## What it demonstrates

- `GovernedLLMClient.stream()` as an `LLMSourceProtocol` (governance
  built in: semaphore / budget / audit).
- The full protocol event sequence: `stream.start` / `stream.delta`
  (content + thinking) / `enrich.marker` / `enrich.placeholder` /
  `enrich.resolved` / `stage.end` / `stream.manifest` / `stream.end`.
- Rich-media placeholders: the model emits `![img]`, the framework
  detects it, dispatches an enrich task, and backfills within the settle
  window.
- Manifest persistence into the trace bundle (`results/`) via the
  protocol result domain.
- Governance kernel + output plane composition: the streamed node is
  still a flow-governed task (snapshot / retry / HITL all work).

## Run

Same prereqs as `examples/real-world` (Redis + OpenAI-compatible
endpoint, `cp .env.example .env`):

    pip install -r requirements.txt
    python run_demo.py

## Cancel semantics (Ctrl+C vs programmatic force)

This demo handles Ctrl+C as a **graceful** interruption: `orchestrator.cancel()`
marks the tree cancelled, the LLM connection stays open, content keeps
being consumed, and the partial output is preserved in the trace bundle
(the cancelled snapshot). That is the default for a reason — you keep the
full content and a clean manifest.

orditect-stream also has a **force** mode, `runner.cancel(stream_id, force=True)`:
the executor coroutine is cancelled, the semaphore is released immediately,
and the httpx connection is dropped. Use it when a slot must be reclaimed
right now (resource pressure, hung LLM). The trade-off: in-flight content
stops mid-stream.

You do not need a separate demo for this — the difference is one argument:
- graceful (default): `await runner.cancel(stream_id=sid)` — sem held until the LLM ends.
- force: `await runner.cancel(stream_id=sid, force=True)` — sem released immediately.

Either way, `get_partial_content(sid)` returns the content generated up to
cancellation, so nothing produced is ever lost.

## Boundary

The demo prints protocol events to stdout. Rendering them (React/Vue/SSE
client) is your own product layer — see the developer guide for an
optional 10-line FastAPI endpoint reference.