"""StreamMux / SeqAllocator 单测。"""
import pytest

from orditect.stream.config import BackpressurePolicy
from orditect.stream.events import EventType
from orditect.stream.exceptions import BackpressureError
from orditect.stream.mux import SeqAllocator, StreamMux


class TestSeqAllocator:
    def test_monotonic(self):
        a = SeqAllocator()
        assert a.next() == 1
        assert a.next() == 2
        assert a.next() == 3
        assert a.current == 3


class TestStreamMux:
    async def test_single_stream_passthrough(self):
        mux = StreamMux()
        mux.register("s1")
        await mux.emit("s1", EventType.STREAM_DELTA, {"data": 1})
        await mux.emit("s1", EventType.STREAM_DELTA, {"data": 2})
        await mux.close_stream("s1")

        out = []
        async for env, et in mux.events():
            out.append((env, et))
        assert len(out) == 2
        assert out[0][0].seq == 1
        assert out[1][0].seq == 2
        assert out[0][0].stream_id == "s1"

    async def test_multi_stream_independent_seq(self):
        mux = StreamMux()
        mux.register("s1")
        mux.register("s2")
        await mux.emit("s1", EventType.STREAM_DELTA, {"n": 1})
        await mux.emit("s2", EventType.STREAM_DELTA, {"n": 100})
        await mux.emit("s1", EventType.STREAM_DELTA, {"n": 2})
        await mux.close_stream("s1")
        await mux.close_stream("s2")

        out = []
        async for env, et in mux.events():
            out.append(env)
        # s1: seq 1,2 ; s2: seq 1
        s1 = [e for e in out if e.stream_id == "s1"]
        s2 = [e for e in out if e.stream_id == "s2"]
        assert [e.seq for e in s1] == [1, 2]
        assert [e.seq for e in s2] == [1]

    async def test_backpressure_fail(self):
        mux = StreamMux(maxsize=1, backpressure=BackpressurePolicy.FAIL)
        mux.register("s1")
        await mux.emit("s1", EventType.STREAM_DELTA, {"n": 1})
        with pytest.raises(BackpressureError):
            await mux.emit("s1", EventType.STREAM_DELTA, {"n": 2})
        await mux.force_close()