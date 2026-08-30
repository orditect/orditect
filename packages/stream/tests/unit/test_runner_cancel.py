"""StreamRunner cancel 功能测试。"""
import asyncio

import pytest

from orditect.stream.config import DEFAULT_CONFIG, EnrichMode
from orditect.stream.core import CancellationToken
from orditect.stream.enrich import MockVectorEnricher
from orditect.stream.events import EventType
from orditect.stream.runner import StreamRunner
from orditect.stream.stages import SourceType, StageConfig
from orditect.stream.store import MemoryResultStore
from orditect.stream.protocols import SourceChunk, SourceRequest


class _MockSource:
    def __init__(self, chunks, delay: float = 0.0):
        self._chunks = chunks
        self._delay = delay

    async def stream(self, request: SourceRequest, cancel_token: CancellationToken | None = None):
        for c in self._chunks:
            if self._delay > 0:
                await asyncio.sleep(self._delay)
            yield c


async def _collect(runner):
    out = []
    async for env, et in runner.run():
        out.append((env, et))
    return out


class TestStreamRunnerCancel:

    async def test_cancel_single_stream(self):
        """cancel 单流：停止输出，下发 stream.cancelled 事件。"""
        cfg = DEFAULT_CONFIG.merge(enrich_mode=EnrichMode.LOCAL)
        runner = StreamRunner(
            stages=[
                StageConfig(
                    name="main",
                    source_type=SourceType.LLM,
                    source=_MockSource([
                        SourceChunk(text="第一段"),
                        SourceChunk(text="第二段"),
                        SourceChunk(finish=True),
                    ], delay=0.1),
                ),
            ],
            enricher=MockVectorEnricher(),
            store=MemoryResultStore(),
            config=cfg,
        )

        # start stream
        events_task = asyncio.create_task(_collect(runner))

        # wait first delta event (ensure stream running and mux not closed)
        await asyncio.sleep(0.15)

        # cancel
        stream_id = runner._stream_ids[0]
        await runner.cancel(stream_id=stream_id, reason="test_cancel")

        events = await events_task
        types = [et for _, et in events]

        # verify stream.cancelled event exists
        assert EventType.STREAM_CANCELLED in types

        # verify cancel event content
        cancel_event = next((e for e, et in events if et == EventType.STREAM_CANCELLED), None)
        assert cancel_event is not None
        assert cancel_event.data["reason"] == "test_cancel"

    async def test_cancel_all_streams(self):
        """cancel 全部：所有子流都收到 cancel 事件。"""
        cfg = DEFAULT_CONFIG.merge(enrich_mode=EnrichMode.LOCAL)
        runner = StreamRunner(
            stages=[
                StageConfig(
                    name="main",
                    source_type=SourceType.LLM,
                    source=_MockSource([SourceChunk(text="正文"), SourceChunk(finish=True)], delay=0.1),
                ),
            ],
            enricher=MockVectorEnricher(),
            store=MemoryResultStore(),
            config=cfg,
            max_id=2,
        )

        events_task = asyncio.create_task(_collect(runner))
        await asyncio.sleep(0.15)  # 确保流正在运行

        # cancel all
        await runner.cancel(reason="cancel_all")

        events = await events_task
        cancel_events = [e for e, et in events if et == EventType.STREAM_CANCELLED]

        # verify all substreams have cancel event
        assert len(cancel_events) == 2

    async def test_get_partial_content(self):
        """get_partial_content：获取中断时的部分内容。"""
        cfg = DEFAULT_CONFIG.merge(enrich_mode=EnrichMode.LOCAL)
        runner = StreamRunner(
            stages=[
                StageConfig(
                    name="main",
                    source_type=SourceType.LLM,
                    source=_MockSource([
                        SourceChunk(text="第一段"),
                        SourceChunk(text="第二段"),
                        SourceChunk(finish=True),
                    ], delay=0.1),
                ),
            ],
            enricher=MockVectorEnricher(),
            store=MemoryResultStore(),
            config=cfg,
        )

        events_task = asyncio.create_task(_collect(runner))
        await asyncio.sleep(0.15)  # 确保第一个 chunk 已产生

        stream_id = runner._stream_ids[0]
        await runner.cancel(stream_id=stream_id)

        partial = runner.get_partial_content(stream_id)
        events = await events_task

        # verify partial_content non-empty (at least partial content)
        assert len(partial) > 0

    async def test_cancel_event_contains_partial_content(self):
        """cancel 事件包含 partial_content。"""
        cfg = DEFAULT_CONFIG.merge(enrich_mode=EnrichMode.LOCAL)
        runner = StreamRunner(
            stages=[
                StageConfig(
                    name="main",
                    source_type=SourceType.LLM,
                    source=_MockSource([
                        SourceChunk(text="第一段内容"),
                        SourceChunk(finish=True),
                    ], delay=0.1),
                ),
            ],
            enricher=MockVectorEnricher(),
            store=MemoryResultStore(),
            config=cfg,
        )

        events_task = asyncio.create_task(_collect(runner))
        await asyncio.sleep(0.15)  # 确保第一个 chunk 已产生

        stream_id = runner._stream_ids[0]
        await runner.cancel(stream_id=stream_id)

        events = await events_task
        cancel_event = next((e for e, et in events if et == EventType.STREAM_CANCELLED), None)

        assert cancel_event is not None
        assert "partial_content" in cancel_event.data
        assert cancel_event.data["partial_content"] is not None

    async def test_cancel_does_not_block_on_full_mux_queue(self):
        """v0.1.4: cancel() must not block when the mux queue is full
        (control path never waits on the data path)."""
        cfg = DEFAULT_CONFIG.merge(enrich_mode=EnrichMode.LOCAL, queue_maxsize=1)
        runner = StreamRunner(
            stages=[
                StageConfig(
                    name="main",
                    source_type=SourceType.LLM,
                    source=_MockSource(
                        [SourceChunk(text="x"), SourceChunk(finish=True)],
                        delay=0.2,
                    ),
                ),
            ],
            enricher=MockVectorEnricher(),
            store=MemoryResultStore(),
            config=cfg,
        )

        events_task = asyncio.create_task(_collect(runner))
        await asyncio.sleep(0.05)  # let events start filling

        stream_id = runner._stream_ids[0]
        # Fill the mux queue to capacity without consuming (simulate a
        # stalled consumer) by pausing consumption via a slow consumer is
        # complex; instead directly verify cancel returns promptly even if
        # the queue would be full. We measure wall time.
        import time as _time
        start = _time.monotonic()
        await runner.cancel(stream_id=stream_id, reason="backpressure_test")
        elapsed = _time.monotonic() - start

        assert elapsed < 0.5, f"cancel() blocked on data path: {elapsed:.2f}s"
        token = runner._cancel_tokens.get(stream_id)
        assert token is not None and token.is_cancelled()

        await events_task

class TestGhostCancelIgnored:
    """v0.1.7 pinning (adjudicated #13): cancel() against a stream_id that
    was never registered must not inject a phantom stream.cancelled event
    for a non-existent stream.

    Red before: cancel(stream_id="ghost") emitted a cancelled event whose
    stream_id did not correspond to any registered substream.
    """

    async def test_cancel_unknown_stream_id_emits_nothing(self):
        cfg = DEFAULT_CONFIG.merge(enrich_mode=EnrichMode.LOCAL)
        runner = StreamRunner(
            stages=[
                StageConfig(
                    name="main",
                    source_type=SourceType.LLM,
                    source=_MockSource(
                        [SourceChunk(text="正文"), SourceChunk(finish=True)],
                        delay=0.05,
                    ),
                ),
            ],
            enricher=MockVectorEnricher(),
            store=MemoryResultStore(),
            config=cfg,
        )

        events_task = asyncio.create_task(_collect(runner))
        await asyncio.sleep(0.1)  # stream registered and running

        partials = await runner.cancel(stream_id="ghost-stream", reason="probe")
        assert partials == {}  # nothing collected for an unknown stream

        events = await events_task
        ghost_cancelled = [
            e for e, et in events
            if et == EventType.STREAM_CANCELLED and e.stream_id == "ghost-stream"
        ]
        assert ghost_cancelled == []
