"""StreamRunner 单测：完整生命周期事件序列。"""
import asyncio

import pytest

from orditect.stream.config import DEFAULT_CONFIG, EnrichMode
from orditect.stream.enrich import MockVectorEnricher
from orditect.stream.events import EventType
from orditect.stream.runner import StreamRunner
from orditect.stream.stages import SourceType, StageConfig
from orditect.stream.store import MemoryResultStore
from orditect.stream.protocols import SourceChunk, SourceRequest
from orditect.stream.core import CancellationToken

class _MockSource:
    def __init__(self, chunks):
        self._chunks = chunks

    async def stream(self, request: SourceRequest, cancel_token: CancellationToken | None = None):
        for c in self._chunks:
            yield c

def _stages(with_marker=False):
    text = "正文![img]完" if with_marker else "正文"
    return [
        StageConfig(name="lead", source_type=SourceType.PASSTHROUGH, content="引导"),
        StageConfig(
            name="main", source_type=SourceType.LLM,
            source=_MockSource([SourceChunk(text=text), SourceChunk(finish=True)]),
        ),
    ]


async def _collect(runner):
    out = []
    async for env, et in runner.run():
        out.append((env, et))
    return out


class TestStreamRunner:
    async def test_full_lifecycle_event_sequence(self):
        cfg = DEFAULT_CONFIG.merge(enrich_mode=EnrichMode.LOCAL)
        runner = StreamRunner(
            stages=_stages(),
            enricher=MockVectorEnricher(),
            store=MemoryResultStore(),
            config=cfg,
        )
        events = await _collect(runner)
        types = [et for _, et in events]

        # sequence: start → delta* → stage.end*2 → manifest → end
        assert types[0] == EventType.STREAM_START
        assert EventType.STREAM_DELTA in types
        assert types.count(EventType.STAGE_END) == 2
        assert EventType.STREAM_MANIFEST in types
        assert types[-1] == EventType.STREAM_END

    async def test_enrich_events_with_marker(self):
        cfg = DEFAULT_CONFIG.merge(
            enrich_mode=EnrichMode.LOCAL, enrich_settle_timeout=1.0
        )
        runner = StreamRunner(
            stages=_stages(with_marker=True),
            enricher=MockVectorEnricher(latency=0.01),
            store=MemoryResultStore(),
            config=cfg,
            loading_url="loading.jpg",
        )
        events = await _collect(runner)
        types = [et for _, et in events]

        assert EventType.ENRICH_MARKER in types
        assert EventType.ENRICH_PLACEHOLDER in types
        # settle window mock completes → resolved seen in stream
        assert EventType.ENRICH_RESOLVED in types

    async def test_max_id_multi_stream(self):
        cfg = DEFAULT_CONFIG.merge(enrich_mode=EnrichMode.LOCAL)
        runner = StreamRunner(
            stages=_stages(),
            enricher=MockVectorEnricher(),
            store=MemoryResultStore(),
            config=cfg,
            max_id=3,
        )
        events = await _collect(runner)
        stream_ids = {env.stream_id for env, _ in events}
        assert len(stream_ids) == 3

        # each substream has start/end
        starts = [e for e, et in events if et == EventType.STREAM_START]
        ends = [e for e, et in events if et == EventType.STREAM_END]
        assert len(starts) == 3
        assert len(ends) == 3

    async def test_manifest_persisted(self):
        store = MemoryResultStore()
        cfg = DEFAULT_CONFIG.merge(enrich_mode=EnrichMode.LOCAL)
        runner = StreamRunner(
            stages=_stages(),
            enricher=MockVectorEnricher(),
            store=store,
            config=cfg,
        )
        events = await _collect(runner)
        sid = events[0][0].stream_id
        manifest = await store.get(sid)
        assert manifest is not None
        assert "stages" in manifest