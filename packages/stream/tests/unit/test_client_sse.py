"""SSEClient / envelope_from_frame 单测。"""
import pytest

from orditect.stream.client import SSEClient, envelope_from_frame


def _sync_stream(chunks):
    for c in chunks:
        yield c


async def _async_stream(chunks):
    for c in chunks:
        yield c


_SSE_TEXT = (
    b"id: s1:1\n"
    b"event: stream.start\n"
    b'data: {"v":1,"stream_id":"s1","seq":1,"ts":1.0,"data":{"stages":["main"]}}\n'
    b"\n"
    b":ping\n"
    b"\n"
    b"id: s1:2\n"
    b"event: stream.delta\n"
    b'data: {"v":1,"stream_id":"s1","seq":2,"ts":1.1,"data":{"kind":"content",\n'
    b'data: "text":"hello"}}\n'
    b"\n"
)


class TestSSEClient:
    def test_sync_parse(self):
        frames = list(SSEClient.parse(_sync_stream([_SSE_TEXT])))
        assert len(frames) == 2
        assert frames[0].event == "stream.start"
        assert frames[0].id == "s1:1"
        assert frames[1].event == "stream.delta"
        # data multi-line reassembly
        assert '"text":"hello"' in frames[1].data or '"text": "hello"' in frames[1].data.replace(" ", "")

    async def test_async_parse(self):
        frames = []
        async for f in SSEClient.aparse(_async_stream([_SSE_TEXT])):
            frames.append(f)
        assert len(frames) == 2
        assert frames[1].event == "stream.delta"

    def test_envelope_from_frame(self):
        frames = list(SSEClient.parse(_sync_stream([_SSE_TEXT])))
        env = envelope_from_frame(frames[1].event, frames[1].data, frames[1].id)
        assert env.stream_id == "s1"
        assert env.seq == 2
        assert env.data["kind"] == "content"

    def test_heartbeat_ignored(self):
        frames = list(SSEClient.parse(_sync_stream([b":ping\n\n"])))
        assert frames == []