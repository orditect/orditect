"""EnrichManager cancel 功能测试（v0.3.0：cancel_tokens 映射签名对齐）。"""
import asyncio

import pytest

from orditect.stream.config import DEFAULT_CONFIG, EnrichMode
from orditect.stream.core import CancellationToken
from orditect.stream.enrich import EnrichManager, MockVectorEnricher
from orditect.stream.mux import StreamMux
from orditect.stream.pipeline import MarkerHit


class TestEnrichManagerCancel:
    async def test_cancel_all(self):
        """cancel_all：取消所有 enrich 任务。"""
        mux = StreamMux()
        mux.register("s1")
        cfg = DEFAULT_CONFIG.merge(enrich_mode=EnrichMode.LOCAL)
        token = CancellationToken()
        manager = EnrichManager(
            enricher=MockVectorEnricher(latency=0.5),  # 长延迟，确保 cancel 在任务完成前
            mux=mux,
            config=cfg,
            cancel_tokens={"s1": token},  # v0.3.0：映射传入
        )

        # trigger enrich task
        hit = MarkerHit(context_text="正文段落")
        await manager.on_hit("s1", "main", hit)

        # wait task start
        await asyncio.sleep(0.05)

        # cancel_all
        await manager.cancel_all()

        # verify task cancelled
        assert len(manager._enrich_tasks) == 0

    async def test_cancel_token_passed_to_enricher(self):
        """cancel_token 按流传递给 enricher（v0.3.0：从映射取所属流的 token）。"""
        mux = StreamMux()
        mux.register("s1")
        cfg = DEFAULT_CONFIG.merge(enrich_mode=EnrichMode.LOCAL)
        token = CancellationToken()

        received_tokens: list = []

        class SpyEnricher:
            async def resolve(self, request, cancel_token=None):
                received_tokens.append(cancel_token)
                await asyncio.sleep(0.01)
                from orditect.stream.protocols import EnrichResult
                from orditect.stream.events import PlaceholderState
                return EnrichResult(url="ok.jpg", state=PlaceholderState.RESOLVED)

        manager = EnrichManager(
            enricher=SpyEnricher(),
            mux=mux,
            config=cfg,
            cancel_tokens={"s1": token},  # v0.3.0：映射传入
        )

        # cancel
        token.cancel("test")

        # trigger enrich task
        hit = MarkerHit(context_text="正文段落")
        await manager.on_hit("s1", "main", hit)

        # wait task complete
        await asyncio.sleep(0.1)

        # verify enricher received token of s1's stream
        assert len(received_tokens) == 1
        assert received_tokens[0] is token