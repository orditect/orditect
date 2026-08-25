"""ThinkingDemux / ChunkSplitter 单测。"""
import pytest

from orditect.stream.config import ThinkingMode
from orditect.stream.pipeline import (
    ChunkSplitter,
    ThinkingDemux,
    aiter_from_iterable,
)
from orditect.stream.protocols import SourceChunk


async def _chunks(items):
    async for c in aiter_from_iterable(items):
        yield c


class TestChunkSplitter:
    async def test_split_order(self):
        chunk = SourceChunk(text="t", thinking="th", references=[{"a": 1}])
        out = []
        async for c in ChunkSplitter().process(_chunks([chunk])):
            out.append(c)
        assert len(out) == 3
        assert out[0].references == [{"a": 1}]
        assert out[1].thinking == "th"
        assert out[2].text == "t"

    async def test_finish_passthrough(self):
        out = []
        async for c in ChunkSplitter().process(_chunks([SourceChunk(finish=True)])):
            out.append(c)
        assert out[-1].finish is True


class TestThinkingDemux:
    async def test_inline_passthrough(self):
        demux = ThinkingDemux(ThinkingMode.INLINE)
        out = []
        async for c in demux.process(_chunks([SourceChunk(thinking="x"), SourceChunk(text="y")])):
            out.append(c)
        assert out[0].thinking == "x"
        assert out[1].text == "y"

    async def test_separate_collects(self):
        demux = ThinkingDemux(ThinkingMode.SEPARATE)
        out = []
        async for c in demux.process(
            _chunks([SourceChunk(thinking="x1"), SourceChunk(text="y"), SourceChunk(thinking="x2")])
        ):
            out.append(c)
        # thinking not passed through
        assert all(c.thinking is None for c in out)
        assert demux.collect() == "x1x2"
        assert demux.pop_collected() == "x1x2"
        assert demux.collect() == ""

    async def test_suppress_drops(self):
        demux = ThinkingDemux(ThinkingMode.SUPPRESS)
        out = []
        async for c in demux.process(_chunks([SourceChunk(thinking="x"), SourceChunk(text="y")])):
            out.append(c)
        assert all(c.thinking is None for c in out)
        assert demux.collect() == ""