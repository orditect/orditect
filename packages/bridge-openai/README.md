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