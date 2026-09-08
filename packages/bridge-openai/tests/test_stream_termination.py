"""Stream termination classification (P0 regression locks).

Pins:
- [DONE] / finish_reason -> "completed", protocol finish chunk appended
- connection closed without markers -> StreamTruncatedError
- zero chunks -> StreamEmptyError
- usage tail chunk (empty choices) is captured into the audit payload
- finish_reason="length" is protocol-completed but stays audit-visible

These tests run against an in-process fake SSE server (no network).
"""
from __future__ import annotations

import pytest

from orditect.bridge.openai import GovernedLLMClient
from orditect.stream.exceptions import StreamEmptyError, StreamTruncatedError

from tests._sse_server import FakeSSEServer, frames_to_body


def _sse_lines(*frames: str, done: bool = True) -> str:
    lines = [f"data: {f}\n\n" for f in frames]
    if done:
        lines.append("data: [DONE]\n\n")
    return "".join(lines)


def _delta(text: str, model: str = "fake-model") -> str:
    return (
        '{"id":"chatcmpl-x","object":"chat.completion.chunk",'
        f'"model":"{model}",'
        '"choices":[{"index":0,"delta":{"content":'
        + __import__("json").dumps(text)
        + '},"finish_reason":null}]}'
    )


_USAGE_TAIL = (
    '{"id":"chatcmpl-x","object":"chat.completion.chunk",'
    '"model":"fake-model","choices":[],'
    '"usage":{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15}}'
)

_FINISH_LENGTH = (
    '{"id":"chatcmpl-x","object":"chat.completion.chunk",'
    '"model":"fake-model",'
    '"choices":[{"index":0,"delta":{},"finish_reason":"length"}]}'
)

_FINISH_STOP = _FINISH_LENGTH.replace('"length"', '"stop"')


def _make_client(server: FakeSSEServer, **kwargs) -> GovernedLLMClient:
    return GovernedLLMClient(
        server.base_url,
        governor=server.governor,
        resource="llm",
        model="fake-model",
        audit_writer=server.audit_writer,
        http_client=server.http_client,
        **kwargs,
    )


class TestTerminationClassification:
    @pytest.mark.asyncio
    async def test_done_marks_completed_and_appends_finish_chunk(self):
        async with FakeSSEServer(_sse_lines(_delta("hello"), _delta(" world"))) as s:
            client = _make_client(s)
            chunks = [c async for c in client.stream(
                messages=[{"role": "user", "content": "hi"}])]
            assert [c.text for c in chunks if c.text] == ["hello", " world"]
            assert chunks[-1].finish is True
            assert s.last_audit["termination"] == "completed"
            assert s.last_audit["stream_chunks"] == 2

    @pytest.mark.asyncio
    async def test_finish_reason_without_done_marks_completed(self):
        """Endpoints that omit [DONE] but send finish_reason still complete."""
        async with FakeSSEServer(
                _sse_lines(_delta("abc"), _FINISH_STOP, done=False)) as s:
            client = _make_client(s)
            chunks = [c async for c in client.stream(
                messages=[{"role": "user", "content": "hi"}])]
            assert chunks[-1].finish is True
            assert s.last_audit["termination"] == "completed"
            assert s.last_audit["finish_reason"] == "stop"

    @pytest.mark.asyncio
    async def test_close_without_markers_raises_truncated(self):
        """Silent connection close mid-stream must fail loudly."""
        async with FakeSSEServer(
                _sse_lines(_delta("partial"), done=False)) as s:
            client = _make_client(s)
            with pytest.raises(StreamTruncatedError) as ei:
                async for _ in client.stream(
                        messages=[{"role": "user", "content": "hi"}]):
                    pass
            assert ei.value.chunks == 1
            assert s.last_audit["termination"] == "truncated"

    @pytest.mark.asyncio
    async def test_zero_chunks_raises_empty(self):
        """A 200 with zero frames is an endpoint incompatibility."""
        async with FakeSSEServer("") as s:
            client = _make_client(s)
            with pytest.raises(StreamEmptyError):
                async for _ in client.stream(
                        messages=[{"role": "user", "content": "hi"}]):
                    pass

    @pytest.mark.asyncio
    async def test_usage_tail_chunk_captured(self):
        """The empty-choices usage chunk must reach the audit payload."""
        body = _sse_lines(_delta("hi"), _USAGE_TAIL)
        async with FakeSSEServer(body) as s:
            client = _make_client(s)
            async for _ in client.stream(
                    messages=[{"role": "user", "content": "hi"}]):
                pass
            assert s.last_audit["usage"]["total_tokens"] == 15

    @pytest.mark.asyncio
    async def test_finish_reason_length_is_completed_but_visible(self):
        """length truncation: protocol-completed, evidence on the record."""
        body = _sse_lines(_delta("cut off mid sen"), _FINISH_LENGTH)
        async with FakeSSEServer(body) as s:
            client = _make_client(s)
            chunks = [c async for c in client.stream(
                messages=[{"role": "user", "content": "hi"}])]
            assert chunks[-1].finish is True
            assert s.last_audit["termination"] == "completed"
            assert s.last_audit["finish_reason"] == "length"

    @pytest.mark.asyncio
    async def test_truncated_stream_still_charges(self):
        """A truncated call consumed real tokens: the audit path runs."""
        async with FakeSSEServer(
                _sse_lines(_delta("partial"), done=False)) as s:
            client = _make_client(s)
            with pytest.raises(StreamTruncatedError):
                async for _ in client.stream(
                        messages=[{"role": "user", "content": "hi"}]):
                    pass
            # audit written exactly once even on the failure path
            assert len(s.audits) == 1