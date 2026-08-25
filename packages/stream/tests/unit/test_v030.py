"""taskstream v0.3.0 钉扎：1b 兜底上限 + 4 按流绑定 token。"""
import asyncio
import dataclasses

import pytest

from orditect.stream.config import DEFAULT_CONFIG, EnrichMode
from orditect.stream.core import CancellationToken
from orditect.stream.enrich import EnrichManager
from orditect.stream.events import PlaceholderState
from orditect.stream.mux import StreamMux
from orditect.stream.pipeline import MarkerHit
from orditect.stream.protocols import EnrichResult, SourceChunk
from orditect.stream.stages import SourceType, StageConfig, StageRunner


class Test1bPostCancelDrainTimeout:
    """1b：cancel 后继续消费有兜底上限。"""

    async def test_drain_timeout_forces_cleanup(self):
        """LLM 挂起 + cancel：超过 post_cancel_drain_timeout 后强制收尾（sem 释放）。"""
        released = []

        class HangingGovernor:
            async def acquire(self, resource, timeout=None):
                return "token-1"

            async def try_acquire(self, resource):
                return "token-1"

            async def release(self, resource, token):
                released.append(resource)

            async def get_usage(self, resource):
                return 0

        class HangingSource:
            """永不结束的 LLM 源（模拟挂起）。"""

            async def stream(self, request, cancel_token=None):
                while True:
                    await asyncio.sleep(0.05)
                    yield SourceChunk(text="x")

        # frozen dataclass: use replace to create config variant with short fallback
        cfg = dataclasses.replace(
            DEFAULT_CONFIG.merge(enrich_mode=EnrichMode.LOCAL),
            post_cancel_drain_timeout=0.3,
        )

        cancel_token = CancellationToken()
        stage_cfg = StageConfig(
            name="main",
            source_type=SourceType.LLM,
            source=HangingSource(),
            resource="default_stream_llm",
        )
        runner = StageRunner(
            stage_cfg, cfg,
            governor=HangingGovernor(),
            cancel_token=cancel_token,
        )

        async def _noop(*args):
            pass

        async def run_stage():
            await runner.run(
                on_text=lambda t: _noop(),
                on_thinking=lambda t: _noop(),
                on_references=lambda r: _noop(),
                on_hit=lambda h: _noop(),
            )

        task = asyncio.create_task(run_stage())
        await asyncio.sleep(0.1)  # 进入消费
        cancel_token.cancel("test")

        # pre-fix: hanging source never ends, run() never returns (test would timeout here)
        # post-fix: fallback limit 0.3s forces cleanup, sem released
        await asyncio.wait_for(task, timeout=2.0)
        assert released == ["default_stream_llm"]


class Test4EnrichTokenPerStream:
    """4：enrich 任务绑定所属流的 token（修复前：永远绑第一条流）。"""

    async def test_second_stream_cancel_reaches_its_enrich_task(self):
        """两条流各触发 enrich：s1 的任务拿 token_s1，s2 的任务拿 token_s2。"""
        mux = StreamMux()
        mux.register("s1")
        mux.register("s2")

        token_s1 = CancellationToken()
        token_s2 = CancellationToken()
        tokens = {"s1": token_s1, "s2": token_s2}

        received: list[tuple[str, CancellationToken | None]] = []

        class SpyEnricher:
            async def resolve(self, request, cancel_token=None):
                received.append((request.stream_id, cancel_token))
                await asyncio.sleep(0.01)
                return EnrichResult(url="ok.jpg", state=PlaceholderState.RESOLVED)

        cfg = DEFAULT_CONFIG.merge(enrich_mode=EnrichMode.LOCAL)
        manager = EnrichManager(
            enricher=SpyEnricher(),
            mux=mux,
            config=cfg,
            cancel_tokens=tokens,
        )

        # two streams each trigger one enrich
        await manager.on_hit("s1", "main", MarkerHit(context_text="ctx1"))
        await manager.on_hit("s2", "main", MarkerHit(context_text="ctx2"))
        await asyncio.sleep(0.1)

        assert len(received) == 2
        s1_token = next(t for sid, t in received if sid == "s1")
        s2_token = next(t for sid, t in received if sid == "s2")
        assert s1_token is token_s1
        assert s2_token is token_s2  # 修复前：两者都是第一条流的 token

class TestStreamEndDuration:
    """v0.3.1：on_stream_end 传真实时长（修复前硬编码 0.0）。"""

    async def test_duration_is_real(self):
        """流有实际耗时时，钩子收到的 duration > 0。"""
        from orditect.stream.runner import StreamRunner
        from orditect.stream.store import MemoryResultStore
        from orditect.stream.enrich import MockVectorEnricher
        from orditect.stream.stages import SourceType, StageConfig

        durations: list[float] = []

        class SpyHooks:
            async def on_stream_start(self, stream_id: str): pass
            async def on_first_token(self, stream_id: str, ttft: float): pass
            async def on_marker(self, stream_id: str, placeholder_id: str): pass
            async def on_resolved(self, stream_id: str, placeholder_id: str, elapsed: float): pass
            async def on_stream_end(self, stream_id: str, duration: float):
                durations.append(duration)
            async def on_error(self, stream_id: str, code: str, message: str): pass

        class SlowSource:
            async def stream(self, request, cancel_token=None):
                await asyncio.sleep(0.15)  # 流有实际耗时
                yield SourceChunk(text="正文")
                yield SourceChunk(finish=True)

        cfg = DEFAULT_CONFIG.merge(enrich_mode=EnrichMode.LOCAL)
        runner = StreamRunner(
            stages=[StageConfig(name="main", source_type=SourceType.LLM, source=SlowSource())],
            enricher=MockVectorEnricher(),
            store=MemoryResultStore(),
            config=cfg,
            hooks=SpyHooks(),
        )

        async for env, et in runner.run():
            pass

        assert len(durations) == 1
        assert durations[0] >= 0.1, f"duration should be real (>=0.1s), got {durations[0]}"