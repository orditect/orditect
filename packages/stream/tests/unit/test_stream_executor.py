"""StreamExecutor 单测：stage 序列 + 事件投放 + 聚合。"""
import pytest

from orditect.stream.config import DEFAULT_CONFIG
from orditect.stream.events import EventType
from orditect.stream.mux import StreamMux
from orditect.stream.protocols import SourceChunk, SourceRequest
from orditect.stream.runner import StreamExecutor
from orditect.stream.stages import SourceType, StageConfig
from orditect.stream.core import CancellationToken

class _MockSource:
    def __init__(self, chunks):
        self._chunks = chunks

    async def stream(self, request: SourceRequest, cancel_token: CancellationToken | None = None):
        for c in self._chunks:
            yield c

class TestStreamExecutor:
    async def test_serial_stages_events(self):
        mux = StreamMux()
        sid = mux.register("s1")

        stages = [
            StageConfig(name="lead", source_type=SourceType.PASSTHROUGH, content="引导"),
            StageConfig(
                name="main", source_type=SourceType.LLM,
                source=_MockSource([SourceChunk(text="正文"), SourceChunk(finish=True)]),
            ),
        ]
        hits_seen = []

        async def on_hit(stream_id, stage, hit):
            hits_seen.append((stream_id, stage, hit))

        executor = StreamExecutor("s1", stages, DEFAULT_CONFIG, mux, on_hit=on_hit)

        import asyncio
        run_task = asyncio.create_task(executor.run())

        out = []
        async def consume():
            async for env, et in mux.events():
                out.append((env, et))

        consume_task = asyncio.create_task(consume())
        result = await run_task
        await mux.close_stream("s1")
        await consume_task

        # event type sequence: delta(lead) + stage.end(lead) + delta(main) + stage.end(main)
        types = [et for _, et in out]
        assert EventType.STREAM_DELTA in types
        assert types.count(EventType.STAGE_END) == 2

        # aggregated result
        assert "lead" in result.stages
        assert "main" in result.stages
        assert result.stages["main"].content == "正文"

    async def test_marker_hit_callback_fired(self):
        mux = StreamMux()
        mux.register("s1")
        stages = [
            StageConfig(
                name="main", source_type=SourceType.LLM,
                source=_MockSource([SourceChunk(text="正文![img]完"), SourceChunk(finish=True)]),
            )
        ]
        hits_seen = []

        async def on_hit(stream_id, stage, hit):
            hits_seen.append(hit)

        import asyncio
        executor = StreamExecutor("s1", stages, DEFAULT_CONFIG, mux, on_hit=on_hit)
        run_task = asyncio.create_task(executor.run())

        async def consume():
            async for env, et in mux.events():
                pass

        consume_task = asyncio.create_task(consume())
        await run_task
        await mux.close_stream("s1")
        await consume_task

        assert len(hits_seen) == 1

from orditect.stream.stream_result import StreamResult as CanonicalStreamResult


class TestStreamResultIdentity:
    async def test_executor_returns_canonical_stream_result(self):
        """v0.1.6 pinning: StreamExecutor.run() must return the canonical
        stream.stream_result.StreamResult — not a module-local shadow
        re-definition (which would break isinstance across the runner /
        finalizer boundary).

        Red before: runner/stream.py both imported and re-defined
        StreamResult, so executors produced a different class object than
        the one runner.py / manifest.py construct.
        """
        mux = StreamMux()
        mux.register("s1")
        stages = [
            StageConfig(name="lead", source_type=SourceType.PASSTHROUGH, content="x"),
        ]

        import asyncio
        executor = StreamExecutor("s1", stages, DEFAULT_CONFIG, mux)
        run_task = asyncio.create_task(executor.run())

        async def consume():
            async for _env, _et in mux.events():
                pass

        consume_task = asyncio.create_task(consume())
        result = await run_task
        await mux.close_stream("s1")
        await consume_task

        assert isinstance(result, CanonicalStreamResult)
        assert type(result) is CanonicalStreamResult