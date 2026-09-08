"""GovernedLLMClient tests (fake governance plane, in-process HTTP fakes).

Covers:
- chat(): governed non-streaming call shape
- stream(): chunk translation, governance lifecycle, audit/cost evidence
- test doubles: FakeGovernor / RecordingAudit / FakeContentWriter /
  MemoryStore-backed audit reads
"""
from __future__ import annotations

import asyncio
import json

import httpx
import pytest

from orditect.adapter.memory import MemoryStore
from orditect.bridge.openai import GovernedLLMClient
from orditect.flow.governor.call import GovernedCallClient


# ---- test doubles ----------------------------------------------------------


class FakeGovernor:
    """Unbounded governor: acquire always succeeds, records usage."""

    def __init__(self) -> None:
        self.acquired: list[str] = []
        self.released: list[str] = []

    async def acquire(self, resource: str, timeout: float | None = None) -> str:
        self.acquired.append(resource)
        return f"tok-{len(self.acquired)}"

    async def try_acquire(self, resource: str) -> str | None:
        return await self.acquire(resource)

    async def release(self, resource: str, token: str) -> None:
        self.released.append(token)

    async def get_usage(self, resource: str) -> int:
        return len(self.acquired) - len(self.released)


class RecordingAudit:
    """Captures AuditEvent objects (flow-side append() contract)."""

    def __init__(self) -> None:
        self.events: list = []

    async def append(self, event) -> None:
        self.events.append(event)


class FakeContentWriter:
    """Content store fake: blobs keyed by a mem:// pointer string."""

    def __init__(self) -> None:
        self.blobs: dict[str, bytes] = {}
        self._n = 0

    async def put(self, data: bytes, content_type: str | None = None):
        self._n += 1
        key = f"mem://c/{self._n}"
        self.blobs[key] = data

        class _Pointer:
            def __init__(self, k: str):
                self._k = k

            def to_payload(self) -> dict:
                return {"pointer": self._k}

        return _Pointer(key)


# ---- helpers ----------------------------------------------------------------


def _sse(lines: list[str], done: bool = True) -> str:
    """Assemble an SSE body from JSON frame strings."""
    body = "".join(f"data: {line}\n\n" for line in lines)
    if done:
        body += "data: [DONE]\n\n"
    return body


def _make_client(handler, store: MemoryStore, **kwargs) -> GovernedLLMClient:
    """Client over an httpx MockTransport with the memory-backed plane."""
    transport = httpx.MockTransport(handler)
    return GovernedLLMClient(
        "http://fake-llm.test/v1",
        governor=FakeGovernor(),
        resource="llm",
        model="fake-model",
        audit_writer=store.audit,
        content_writer=store.content,
        http_client=httpx.AsyncClient(transport=transport, timeout=30.0),
        **kwargs,
    )


async def _drain_stream(stream):
    """Consume a governed stream to completion; return all chunks.

    Uses __aiter__() explicitly: stream() is an async generator function
    and some type checkers mis-resolve the async-for protocol on the
    AsyncIterator return annotation.
    """
    chunks = []
    ait = stream.__aiter__()
    try:
        while True:
            try:
                chunks.append(await ait.__anext__())
            except StopAsyncIteration:
                break
    finally:
        aclose = getattr(stream, "aclose", None)
        if aclose is not None:
            await aclose()
    return chunks


# ---- chat -------------------------------------------------------------------


class TestChat:
    @pytest.mark.asyncio
    async def test_chat_returns_endpoint_result(self):
        async def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={
                "id": "chatcmpl-1",
                "model": "fake-model",
                "choices": [{
                    "index": 0,
                    "message": {"role": "assistant", "content": "hi"},
                    "finish_reason": "stop",
                }],
                "usage": {"prompt_tokens": 3, "completion_tokens": 2,
                          "total_tokens": 5},
            })

        store = MemoryStore()
        client = _make_client(handler, store)
        result = await client.chat(
            messages=[{"role": "user", "content": "hello"}])
        await client.aclose()

        assert result["choices"][0]["message"]["content"] == "hi"
        events = store.audit._events
        assert len(events) == 1
        ev = next(iter(events.values()))
        assert ev.payload.get("usage", {}).get("total_tokens") == 5
        assert ev.payload.get("finish_reason") == "stop"

    @pytest.mark.asyncio
    async def test_chat_http_error_raises(self):
        async def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(500, text="boom")

        store = MemoryStore()
        client = _make_client(handler, store)
        with pytest.raises(httpx.HTTPStatusError):
            await client.chat(
                messages=[{"role": "user", "content": "hello"}])
        await client.aclose()


# ---- streaming ----------------------------------------------------------------


class TestStreaming:
    @pytest.mark.asyncio
    async def test_stream_yields_translated_chunks(self):
        async def handler(request: httpx.Request) -> httpx.Response:
            lines = [
                json.dumps({"choices": [{"delta": {"content": "he"}}]}),
                json.dumps({"choices": [{"delta": {"content": "llo"}}]}),
            ]
            return httpx.Response(
                200, text=_sse(lines),
                headers={"Content-Type": "text/event-stream"},
            )

        store = MemoryStore()
        client = _make_client(handler, store)
        chunks = await _drain_stream(
            client.stream(messages=[{"role": "user", "content": "hi"}]))
        await client.aclose()

        texts = [c.text for c in chunks if c.text]
        assert texts == ["he", "llo"]
        # the fixed bridge appends a protocol finish chunk at [DONE]
        assert chunks[-1].finish is True

    @pytest.mark.asyncio
    async def test_stream_reasoning_goes_to_thinking_channel(self):
        async def handler(request: httpx.Request) -> httpx.Response:
            lines = [
                json.dumps({"choices": [{"delta": {
                    "reasoning_content": "pondering"}}]}),
                json.dumps({"choices": [{"delta": {"content": "answer"}}]}),
            ]
            return httpx.Response(
                200, text=_sse(lines),
                headers={"Content-Type": "text/event-stream"},
            )

        store = MemoryStore()
        client = _make_client(handler, store)
        chunks = await _drain_stream(
            client.stream(messages=[{"role": "user", "content": "hi"}]))
        await client.aclose()

        thinking = [c.thinking for c in chunks if c.thinking]
        texts = [c.text for c in chunks if c.text]
        assert thinking == ["pondering"]
        assert texts == ["answer"]

    @pytest.mark.asyncio
    async def test_stream_break_marks_interrupted_and_pointerizes(self):
        """Break without a cancel token = external interruption (v0.1.8
        semantics): partial bytes are pointer-ized, the record carries
        interrupted, and it is NOT marked cancelled."""
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
        assert "cancelled" not in ev.payload
        assert ev.payload["interrupted"] is True
        assert "pointer" in ev.payload
        await client.aclose()

    @pytest.mark.asyncio
    async def test_stream_break_with_cancelled_token_marks_cancelled_and_pointerizes_partial(self):
        """Break with a flipped cancel token = true cancellation.

        The token flips DURING consumption (the realistic HITL shape:
        cancel arrives after the stream started). The record is marked
        cancelled, partial bytes are pointer-ized, and nothing is
        charged (mirrors call()'s cancel path).
        """
        audit = RecordingAudit()
        content = FakeContentWriter()

        class _Token:
            def __init__(self):
                self._cancelled = False

            def cancel(self):
                self._cancelled = True

            async def is_cancelled(self):
                return self._cancelled

        token = _Token()

        async def gen():
            for i in range(100):
                yield i

        client = GovernedCallClient(
            FakeGovernor(), "res", audit_writer=audit, content_writer=content
        )
        count = 0
        async for _ in client.call_streaming(
            handler=lambda: gen(),
            partial_fn=lambda: b"partial-data",
            cancel_token=token,
        ):
            count += 1
            if count == 2:
                token.cancel()  # cancel arrives mid-stream
                break

        for _ in range(50):
            if audit.events:
                break
            await asyncio.sleep(0.01)

        ev = audit.events[0]
        assert ev.payload["cancelled"] is True
        assert list(content.blobs.values()) == [b"partial-data"]

    @pytest.mark.asyncio
    async def test_cost_fn_holder_carries_no_internal_fields(self):
        """v0.1.6 pinning: the result holder handed to cost_fn contains only
        endpoint vocabulary plus the streaming evidence fields — never the
        internal _latency_ms that C5 removed from the non-streaming path."""
        async def handler(request: httpx.Request) -> httpx.Response:
            lines = [
                json.dumps({"choices": [{"delta": {"content": "x"}}]}),
                json.dumps({
                    "model": "gpt-4o",
                    "choices": [{"delta": {}}],
                    "usage": {"total_tokens": 5, "prompt_tokens": 2,
                              "completion_tokens": 3},
                }),
            ]
            return httpx.Response(
                200, text=_sse(lines),
                headers={"Content-Type": "text/event-stream"},
            )

        store = MemoryStore()
        seen: list = []
        client = _make_client(handler, store,
                              cost_fn=lambda r: seen.append(r) or 5)
        await _drain_stream(
            client.stream(messages=[{"role": "user", "content": "hi"}]))
        await client.aclose()

        assert seen and "_latency_ms" not in seen[-1]
        # endpoint vocabulary plus the streaming evidence fields
        # (termination/stream_chunks/finish_reason) — never internal fields.
        assert set(seen[-1].keys()) <= {
            "usage", "model", "termination", "stream_chunks",
            "finish_reason",
        }

    @pytest.mark.asyncio
    async def test_stream_aclose_releases_governor_token(self):
        """v0.1.7 pin: an explicit aclose deterministically releases the
        semaphore instead of relying on GC timing."""
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
        transport = httpx.MockTransport(handler)
        governor = FakeGovernor()
        client = GovernedLLMClient(
            "http://fake-llm.test/v1",
            governor=governor,
            resource="llm",
            model="fake-model",
            audit_writer=store.audit,
            http_client=httpx.AsyncClient(transport=transport, timeout=30.0),
        )
        stream = client.stream(messages=[{"role": "user", "content": "hi"}])
        async for _ in stream:
            break
        await stream.aclose()
        for _ in range(50):
            if governor.released:
                break
            await asyncio.sleep(0.01)
        assert governor.acquired == ["llm"]
        assert len(governor.released) == 1
        await client.aclose()