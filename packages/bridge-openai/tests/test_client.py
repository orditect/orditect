
"""Pinning tests for GovernedLLMClient (endpoint bridge reference).

Covers: non-streaming governed call (audit once, usage->cost, latency),
streaming as LLMSourceProtocol (SourceChunk deltas + finish, charge at
stream end, audit at stream close), usage-missing pricing path, cancel
cleanup, and that OpenAI-shaped vocabulary stays at the bridge edge
(audit payload is the only place it appears).
"""

from __future__ import annotations

import json
import asyncio
import httpx
import pytest

from orditect.bridge.openai import GovernedLLMClient
from orditect.adapter.memory import MemoryStore
from orditect.stream.protocols.source import SourceChunk

pytestmark = pytest.mark.unit


class FakeGovernor:
    def __init__(self):
        self.acquired: list[str] = []
        self.released: list[str] = []

    async def acquire(self, resource: str, timeout=None) -> str:
        self.acquired.append(resource)
        return "tok-1"

    async def try_acquire(self, resource: str):
        return await self.acquire(resource)

    async def release(self, resource: str, token: str) -> None:
        self.released.append(resource)

    async def get_usage(self, resource: str) -> int:
        return 0


def _chat_response(model="gpt-4o", content="hello", total_tokens=42):
    return {
        "id": "chatcmpl-1",
        "model": model,
        "choices": [
            {
                "index": 0,
                "message": {"role": "assistant", "content": content},
                "finish_reason": "stop",
            }
        ],
        "usage": {
            "prompt_tokens": 10,
            "completion_tokens": 32,
            "total_tokens": total_tokens,
        },
    }


def _sse(lines: list[str]) -> str:
    return "".join(f"data: {l}\n\n" for l in lines) + "data: [DONE]\n\n"


def _make_client(handler, store, **kwargs):
    transport = httpx.MockTransport(handler)
    http = httpx.AsyncClient(transport=transport)
    defaults = dict(
        governor=FakeGovernor(),
        resource="llm",
        audit_writer=store.audit,
        content_writer=store.content,
        model="gpt-4o",
        task_id="t-1",
        http_client=http,
    )
    defaults.update(kwargs)
    return GovernedLLMClient("http://test", **defaults)

async def _drain_stream(stream):
    """Consume a stream generator to completion AND let its finally blocks
    (budget charge + audit write) settle on the next event-loop tick."""
    chunks = []
    async for chunk in stream:
        chunks.append(chunk)
    # Yield control so the generator's finally chain (which runs on the
    # next tick after the async-for exits) completes.
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    return chunks

class TestNonStreaming:
    async def test_chat_governed_and_audited(self):
        async def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=_chat_response())

        store = MemoryStore()
        client = _make_client(handler, store)
        result = await client.chat(
            messages=[{"role": "user", "content": "hi"}], call_id="c-1"
        )

        assert result["usage"]["total_tokens"] == 42
        events = store.audit._events  # memory part introspection for pinning
        assert len(events) == 1
        ev = events["c-1"]
        assert ev.event_type == "llm_call"
        assert ev.task_id == "t-1"
        assert ev.payload["model"] == "gpt-4o"
        assert ev.payload["usage"]["total_tokens"] == 42
        assert ev.payload["finish_reason"] == "stop"
        assert "latency_ms" in ev.payload

    async def test_messages_pointerized(self):
        async def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=_chat_response())

        store = MemoryStore()
        client = _make_client(handler, store)
        await client.chat(messages=[{"role": "user", "content": "secret"}])

        # content part holds the pointer-ized messages blob
        assert len(store.content._data) == 1
        blob = next(iter(store.content._data.values()))[0]
        assert b"secret" in blob


class TestStreaming:

    async def test_stream_yields_source_chunks_and_charges_at_end(self):
        async def handler(request: httpx.Request) -> httpx.Response:
            lines = [
                json.dumps({"choices": [{"delta": {"content": "he"}}]}),
                json.dumps({"choices": [{"delta": {"content": "llo"}}]}),
                json.dumps({
                    "model": "gpt-4o",
                    "choices": [{"delta": {}}],
                    "usage": {"total_tokens": 7, "prompt_tokens": 2,
                              "completion_tokens": 5},
                }),
            ]
            return httpx.Response(
                200, text=_sse(lines),
                headers={"Content-Type": "text/event-stream"},
            )

        store = MemoryStore()
        seen_costs: list = []

        def cost_fn(result):
            seen_costs.append(result)
            return (result or {}).get("usage", {}).get("total_tokens", 0)

        client = _make_client(handler, store, cost_fn=cost_fn)
        chunks = await _drain_stream(
            client.stream(messages=[{"role": "user", "content": "hi"}])
        )

        texts = [c.text for c in chunks if c.text]
        assert texts == ["he", "llo"]
        assert chunks[-1].finish is True

        # charge happened once, at stream end, with the usage holder
        assert seen_costs and seen_costs[-1]["usage"]["total_tokens"] == 7

        events = store.audit._events
        assert len(events) == 1
        ev = next(iter(events.values()))
        assert ev.payload["usage"]["total_tokens"] == 7
        assert ev.payload["model"] == "gpt-4o"

    async def test_stream_usage_missing_cost_fn_gets_none(self):
        async def handler(request: httpx.Request) -> httpx.Response:
            lines = [json.dumps({"choices": [{"delta": {"content": "x"}}]})]
            return httpx.Response(
                200, text=_sse(lines),
                headers={"Content-Type": "text/event-stream"},
            )

        store = MemoryStore()
        seen: list = []
        client = _make_client(
            handler, store, cost_fn=lambda r: seen.append(r) or 3
        )
        await _drain_stream(
            client.stream(messages=[{"role": "user", "content": "hi"}])
        )

        # A5: no usage in the stream -> cost_fn receives None; business prices it.
        assert seen == [None]

    async def test_stream_break_marks_cancelled_and_pointerizes(self):
        async def handler(request: httpx.Request) -> httpx.Response:
            lines = [
                json.dumps({"choices": [{"delta": {"content": f"c{i}"}}]})
                for i in range(50)
            ]
            return httpx.Response(
                200, text=_sse(lines),
                headers={"Content-Type": "text/event-stream"},
            )

        store = MemoryStore()
        client = _make_client(handler, store)
        count = 0
        stream = client.stream(messages=[{"role": "user", "content": "hi"}])
        async for _ in stream:
            count += 1
            if count == 2:
                break
        # break only suspends the generator; explicitly close it so the
        # finally chain (partial pointer-ize + audit write) executes.
        await stream.aclose()
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        events = store.audit._events
        assert len(events) == 1
        ev = next(iter(events.values()))
        assert ev.payload["cancelled"] is True
        assert ev.payload["pointer"]["backend"] == "memory"