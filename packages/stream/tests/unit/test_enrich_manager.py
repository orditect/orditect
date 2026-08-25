
"""EnrichManager 单测：on_hit 派发 + settle 窗口。"""
import asyncio

import pytest

from orditect.stream.config import DEFAULT_CONFIG, EnrichMode
from orditect.stream.enrich import EnrichManager, MockVectorEnricher
from orditect.stream.events import EventType, PlaceholderState
from orditect.stream.mux import StreamMux
from orditect.stream.pipeline import MarkerHit


async def _setup(enricher=None, mode=EnrichMode.LOCAL, loading="loading.jpg"):
    mux = StreamMux()
    mux.register("s1")
    cfg = DEFAULT_CONFIG.merge(enrich_mode=mode)
    mgr = EnrichManager(
        enricher=enricher or MockVectorEnricher(latency=0.05),
        mux=mux, config=cfg, loading_url=loading,
    )
    return mux, mgr


async def _drain(mux):
    out = []
    async for env, et in mux.events():
        out.append((env, et))
    return out


class TestEnrichManager:
    async def test_on_hit_emits_marker_and_placeholder(self):
        mux, mgr = await _setup()
        hit = MarkerHit(context_text="正文段落")

        consume = asyncio.create_task(_drain(mux))
        await mgr.on_hit("s1", "main", hit)
        # wait local dispatch complete
        await asyncio.sleep(0.1)
        await mux.force_close()
        events = await consume

        types = [et for _, et in events]
        assert EventType.ENRICH_MARKER in types
        assert EventType.ENRICH_PLACEHOLDER in types
        # hit backfilled with placeholder_id
        assert hit.placeholder_id is not None
        assert hit.placeholder_id.startswith("ph_")

    async def test_local_dispatch_resolved_in_registry(self):
        mux, mgr = await _setup()
        hit = MarkerHit(context_text="正文段落")
        consume = asyncio.create_task(_drain(mux))
        await mgr.on_hit("s1", "main", hit)
        await asyncio.sleep(0.15)  # 等 mock latency
        await mux.force_close()
        await consume

        rec = mgr.registry.get(hit.placeholder_id)
        assert rec.state is PlaceholderState.RESOLVED
        assert rec.url.endswith(f"{hit.placeholder_id}.jpg")
        assert rec.task_ref.startswith("local:")

    async def test_settle_emits_resolved_within_window(self):
        mux, mgr = await _setup()
        hit = MarkerHit(context_text="正文段落")
        consume = asyncio.create_task(_drain(mux))
        await mgr.on_hit("s1", "main", hit)
        await mgr.settle(timeout=1.0)  # 窗口内应 resolve
        await mux.force_close()
        events = await consume

        types = [et for _, et in events]
        assert EventType.ENRICH_RESOLVED in types

    async def test_settle_timeout_marks_failed(self):
        """v0.3.2 翻转：local 模式超窗落 failed + fallback（不再 pending 画饼）。"""
        mux, mgr = await _setup(enricher=MockVectorEnricher(latency=5.0))
        hit = MarkerHit(context_text="正文段落")
        consume = asyncio.create_task(_drain(mux))
        await mgr.on_hit("s1", "main", hit)
        await mgr.settle(timeout=0.05)  # 窗口太短，resolve 不了
        await mux.force_close()
        await consume

        rec = mgr.registry.get(hit.placeholder_id)
        # v0.3.2: local has no delegation channel, timeout truthfully marks failed
        assert rec.state is PlaceholderState.FAILED
        assert rec.fallback_url == "loading.jpg"

    async def test_failed_path(self):
        mux, mgr = await _setup(
            enricher=MockVectorEnricher(latency=0.01, fail_on_context="炸")
        )
        hit = MarkerHit(context_text="这段会炸")
        consume = asyncio.create_task(_drain(mux))
        await mgr.on_hit("s1", "main", hit)
        await asyncio.sleep(0.1)
        await mux.force_close()
        await consume

        rec = mgr.registry.get(hit.placeholder_id)
        assert rec.state is PlaceholderState.FAILED