
"""全链路 e2e：mock source → runner → SSE 字节流验证（无需 Redis/FastAPI）。"""
import asyncio

import pytest

from orditect.stream.config import DEFAULT_CONFIG, EnrichMode, ThinkingMode
from orditect.stream.enrich import MockVectorEnricher
from orditect.stream.events import EventType
from orditect.stream.runner import StreamRunner
from orditect.stream.sse import encode_envelope
from orditect.stream.stages import SourceType, StageConfig
from orditect.stream.store import MemoryResultStore
from orditect.stream.protocols import SourceChunk, SourceRequest
from orditect.stream.core import CancellationToken

pytestmark = pytest.mark.integration


class _MockLLMSource:
    def __init__(self, chunks):
        self._chunks = chunks

    async def stream(self, request: SourceRequest, cancel_token: CancellationToken | None = None):
        for c in self._chunks:
            yield c


class TestE2E:
    async def test_full_pipeline_to_sse_bytes(self):
        """从 runner 到 SSE 字节流：帧格式正确。"""
        chunks = [
            SourceChunk(text="第一段。"),
            SourceChunk(text="![img]"),
            SourceChunk(text="第二段。"),
            SourceChunk(finish=True),
        ]
        cfg = DEFAULT_CONFIG.merge(
            enrich_mode=EnrichMode.LOCAL, enrich_settle_timeout=0.5
        )
        runner = StreamRunner(
            stages=[
                StageConfig(name="main", source_type=SourceType.LLM,
                            source=_MockLLMSource(chunks)),
            ],
            enricher=MockVectorEnricher(latency=0.01),
            store=MemoryResultStore(),
            config=cfg,
            loading_url="https://oss/loading.jpg",
        )

        frames = []
        async for env, et in runner.run():
            frames.append(encode_envelope(env, et))

        # all frames are valid bytes and end with \n\n
        assert all(isinstance(f, bytes) for f in frames)
        assert all(f.endswith(b"\n\n") for f in frames)
        # contains id: and event: fields
        joined = b"".join(frames)
        assert b"id: " in joined
        assert b"event: stream.start" in joined
        assert b"event: enrich.marker" in joined
        assert b"event: stream.end" in joined

    async def test_thinking_modes_e2e(self):
        """三档 thinking 模式 e2e。"""
        chunks = [
            SourceChunk(thinking="想一下"),
            SourceChunk(text="正文"),
            SourceChunk(finish=True),
        ]

        # inline: thinking into delta
        cfg = DEFAULT_CONFIG.merge(thinking_mode=ThinkingMode.INLINE)
        runner = StreamRunner(
            stages=[StageConfig(name="m", source_type=SourceType.LLM,
                                source=_MockLLMSource(chunks))],
            enricher=MockVectorEnricher(),
            store=MemoryResultStore(),
            config=cfg,
        )
        kinds = []
        async for env, et in runner.run():
            if et == EventType.STREAM_DELTA:
                kinds.append(env.data.get("kind"))
        assert "thinking" in kinds
        assert "content" in kinds

        # separate: thinking not into delta, into stage.end
        cfg2 = DEFAULT_CONFIG.merge(thinking_mode=ThinkingMode.SEPARATE)
        runner2 = StreamRunner(
            stages=[StageConfig(name="m", source_type=SourceType.LLM,
                                source=_MockLLMSource(chunks))],
            enricher=MockVectorEnricher(),
            store=MemoryResultStore(),
            config=cfg2,
        )
        delta_kinds = []
        stage_end_data = None
        async for env, et in runner2.run():
            if et == EventType.STREAM_DELTA:
                delta_kinds.append(env.data.get("kind"))
            if et == EventType.STAGE_END:
                stage_end_data = env.data
        assert "thinking" not in delta_kinds
        assert stage_end_data["result"]["thinking"] == "想一下"