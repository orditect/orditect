"""taskstream v0.3.3 钉扎：T1 references 链路 / T2 cancel 拦截 /
T3 终态事件放行 / T4 char_offset / T6 partial 纯度 / T7 终态不可逆。"""
import asyncio

import pytest

from orditect.stream.config import DEFAULT_CONFIG, EnrichMode, ThinkingMode
from orditect.stream.core import CancellationToken
from orditect.stream.enrich import MockVectorEnricher, PlaceholderRecord, PlaceholderRegistry
from orditect.stream.events import EventType, PlaceholderState
from orditect.stream.pipeline import MarkerDetector, aiter_from_iterable
from orditect.stream.protocols import SourceChunk, SourceRequest
from orditect.stream.runner import StreamRunner
from orditect.stream.stages import SourceType, StageConfig, StageRunner
from orditect.stream.store import MemoryResultStore


class _MockSource:
    def __init__(self, chunks, delay: float = 0.0):
        self._chunks = chunks
        self._delay = delay

    async def stream(self, request: SourceRequest, cancel_token: CancellationToken | None = None):
        for c in self._chunks:
            if self._delay > 0:
                await asyncio.sleep(self._delay)
            yield c


class TestT1ReferencesPipeline:
    """T1：references 链路接通（修复前被 detector 吞掉）。"""

    async def test_references_event_reaches_output(self):
        """StageRunner 端到端：references chunk → on_references 回调真实触发。"""
        chunks = [
            SourceChunk(references=[{"doc": "a.pdf"}]),
            SourceChunk(text="正文"),
            SourceChunk(finish=True),
        ]
        cfg = StageConfig(name="main", source_type=SourceType.LLM, source=_MockSource(chunks))
        runner = StageRunner(cfg, DEFAULT_CONFIG)

        refs_seen = []
        texts = []

        async def _noop(*args):
            pass

        await runner.run(
            on_text=lambda t: texts.append(t) or _noop(),
            on_thinking=lambda t: _noop(),
            on_references=lambda r: refs_seen.extend(r) or _noop(),
            on_hit=lambda h: _noop(),
        )

        # pre-fix: refs_seen == [] (swallowed by detector continue)
        assert refs_seen == [{"doc": "a.pdf"}]
        assert "".join(texts) == "正文"

    async def test_references_e2e_delta_event(self):
        """runner 级：stream.delta kind=references 事件真实发出。"""
        chunks = [
            SourceChunk(references=[{"doc": "a.pdf"}]),
            SourceChunk(text="正文"),
            SourceChunk(finish=True),
        ]
        runner = StreamRunner(
            stages=[StageConfig(name="main", source_type=SourceType.LLM, source=_MockSource(chunks))],
            enricher=MockVectorEnricher(),
            store=MemoryResultStore(),
            config=DEFAULT_CONFIG.merge(enrich_mode=EnrichMode.LOCAL),
        )
        kinds = []
        async for env, et in runner.run():
            if et == EventType.STREAM_DELTA:
                kinds.append(env.data.get("kind"))

        # pre-fix: "references" not in kinds
        assert "references" in kinds
        assert "content" in kinds


class TestT2CancelStopsThinking:
    """T2：cancel 后 thinking delta 同样停止。"""

    async def test_thinking_stops_after_cancel(self):
        """INLINE 模式：cancel 后 thinking 不再下发（与 content 同语义）。

        时序说明：thinking 在 detector 之前被 _intercept 拦截回调，
        不等 detector 冲刷——cancel 必须在源头侧注入（source 第一个
        chunk 之后），而非 on_text 回调里（on_text 要等 detector 冲刷，
        晚于 thinking 的拦截点）。
        """
        cancel_token = CancellationToken()

        class CancellingSource:
            async def stream(self, request: SourceRequest, cancel_token=None):
                yield SourceChunk(text="第一段")
                cancel_token.cancel("test")  # 源头注入：第二个 chunk 前取消
                yield SourceChunk(thinking="取消后的思维链")
                yield SourceChunk(text="第二段")
                yield SourceChunk(finish=True)

        cfg = StageConfig(name="main", source_type=SourceType.LLM, source=CancellingSource())
        runner = StageRunner(
            cfg,
            DEFAULT_CONFIG.merge(thinking_mode=ThinkingMode.INLINE),
            cancel_token=cancel_token,
        )

        texts, thinkings = [], []

        async def _noop(*args):
            pass

        await runner.run(
            on_text=lambda t: texts.append(t) or _noop(),
            on_thinking=lambda t: thinkings.append(t) or _noop(),
            on_references=lambda r: _noop(),
            on_hit=lambda h: _noop(),
        )

        # thinking intercepted (T2); text first paragraph already in detector buffer before cancel,
        # after cancel output loop continue no longer delivered — both conform "stop output" semantics
        assert thinkings == []


class TestT3TerminalEventsAfterCancel:
    """T3：cancel 后 manifest/end 终态事件送达。"""

    async def test_manifest_and_end_delivered_after_cancel(self):
        chunks = [SourceChunk(text=f"段{i}") for i in range(10)] + [SourceChunk(finish=True)]
        runner = StreamRunner(
            stages=[StageConfig(name="main", source_type=SourceType.LLM,
                                source=_MockSource(chunks, delay=0.05))],
            enricher=MockVectorEnricher(),
            store=MemoryResultStore(),
            config=DEFAULT_CONFIG.merge(enrich_mode=EnrichMode.LOCAL),
        )

        async def consume():
            out = []
            async for env, et in runner.run():
                out.append(et)
            return out

        task = asyncio.create_task(consume())
        await asyncio.sleep(0.15)  # 流运行中

        stream_id = runner._stream_ids[0]
        await runner.cancel(stream_id=stream_id, reason="t3_test")

        types = await task
        # pre-fix: after cancel manifest/end swallowed, not in types
        assert EventType.STREAM_CANCELLED in types
        assert EventType.STREAM_MANIFEST in types
        assert EventType.STREAM_END in types
        # terminal order: cancelled < manifest < end
        assert (types.index(EventType.STREAM_CANCELLED)
                < types.index(EventType.STREAM_MANIFEST)
                < types.index(EventType.STREAM_END))


class TestT4CharOffsetAtFinish:
    """T4：finish 分支 hit 的 char_offset 不错位。"""

    async def test_trailing_marker_offset_correct(self):
        """尾部 marker：offset = marker 前文本长度（不含 marker 后文本）。"""
        chunks = [
            SourceChunk(text="第一段。"),
            SourceChunk(text="![img]"),
            SourceChunk(text="第二段。"),
            SourceChunk(text="![img]"),  # 尾部 marker（finish 时仍在 buffer）
            SourceChunk(finish=True),
        ]
        det = MarkerDetector()

        from orditect.stream.pipeline import MarkedChunk
        events: list[MarkedChunk] = []
        async for out in det.process(aiter_from_iterable(chunks)):
            events.append(out)

        # simulate StageRunner offset backfill logic: content accumulates to length when hit arrives
        content_len = 0
        offsets = []
        for ev in events:
            if ev.text:
                content_len += len(ev.text)
            for hit in ev.hits:
                offsets.append(content_len)  # StageRunner: hit.char_offset = len(content)

        # first marker after "First paragraph."; second after "First paragraph.Second paragraph."
        # pre-fix (batching): two hits batched to final yield, offsets all = full text length (misplaced)
        assert offsets == [len("第一段。"), len("第一段。第二段。")]


class TestT6PartialContentPurity:
    """T6：partial_content 只含 content（不混 thinking）。"""

    async def test_partial_excludes_thinking(self):
        """时序说明：cancel 后消费循环跳过未消费的 delta——
        partial 的可靠断言点只能是"不含 thinking"（负向断言），
        不能断言"必含某段正文"（cancel 时机与 mux 消费进度有竞态）。
        用 finish 前不 cancel、跑到中段再 cancel 的双段设计：
        等"正文一"的 delta 确实被消费后再 cancel。
        """
        first_delta_seen = asyncio.Event()

        chunks = [
            SourceChunk(text="正文一"),
            SourceChunk(thinking="思维链"),
            SourceChunk(text="正文二"),
            SourceChunk(finish=True),
        ]
        runner = StreamRunner(
            stages=[StageConfig(name="main", source_type=SourceType.LLM,
                                source=_MockSource(chunks, delay=0.05))],
            enricher=MockVectorEnricher(),
            store=MemoryResultStore(),
            config=DEFAULT_CONFIG.merge(
                enrich_mode=EnrichMode.LOCAL, thinking_mode=ThinkingMode.INLINE,
            ),
        )

        async def consume():
            async for env, et in runner.run():
                if (et == EventType.STREAM_DELTA
                        and env.data.get("kind") == "content"
                        and "正文一" in env.data.get("text", "")):
                    first_delta_seen.set()

        task = asyncio.create_task(consume())

        # wait for "Main text one" delta indeed processed by consumer loop (partial collected) then cancel
        assert await asyncio.wait_for(first_delta_seen.wait(), timeout=3.0)
        await asyncio.sleep(0.08)  # Let thinking deltas also flow through the consumer loop.

        stream_id = runner._stream_ids[0]
        await runner.cancel(stream_id=stream_id)
        await task

        partial = runner.get_partial_content(stream_id)
        # positive: consumed "Main text one" in partial
        assert "正文一" in partial
        # negative (core acceptance): thinking not mixed in — regardless of thinking delta
        # before/after cancel, should not appear in partial_content
        assert "思维链" not in partial

class TestT7TerminalIrreversible:
    """T7: PlaceholderRegistry final state is irreversible."""

    async def test_failed_not_overwritten_by_late_resolved(self):
        """After settle times out and becomes failed, a later resolved does not override."""
        reg = PlaceholderRegistry()
        await reg.register(PlaceholderRecord(
            placeholder_id="ph_t7", stream_id="s1", stage="main",
            context_text="ctx", loading_url="l.jpg",
        ))
        await reg.mark_failed("ph_t7", "settle timeout", "l.jpg")

        rec = await reg.mark_resolved("ph_t7", "real.jpg")
        # pre-fix: state overwritten to RESOLVED, contradicts failed in already delivered manifest
        assert rec.state is PlaceholderState.FAILED
        assert rec.url is None

    async def test_resolved_not_overwritten_by_late_failed(self):
        reg = PlaceholderRegistry()
        await reg.register(PlaceholderRecord(
            placeholder_id="ph_t7b", stream_id="s1", stage="main",
            context_text="ctx", loading_url="l.jpg",
        ))
        await reg.mark_resolved("ph_t7b", "real.jpg")

        rec = await reg.mark_failed("ph_t7b", "late error")
        assert rec.state is PlaceholderState.RESOLVED
        assert rec.url == "real.jpg"