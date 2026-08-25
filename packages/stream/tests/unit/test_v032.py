"""taskstream v0.3.2 钉扎：enrich 委托链路闭环 + 释放路径 + 契约防护。"""
import asyncio

import pytest

from orditect.stream.config import DEFAULT_CONFIG, EnrichMode
from orditect.stream.client import ManifestResolver
from orditect.stream.core import CancellationToken
from orditect.stream.enrich import EnrichManager, MockVectorEnricher
from orditect.stream.events import EventType, PlaceholderState
from orditect.stream.mux import StreamMux
from orditect.stream.pipeline import MarkerHit
from orditect.stream.runner import StreamRunner
from orditect.stream.stages import SourceType, StageConfig
from orditect.stream.store import MemoryResultStore
from orditect.stream.protocols import SourceChunk, SourceRequest


class TestTaskRefContract:
    """簇 B：task_ref 确定性 ID 约定。"""

    async def test_taskflow_mode_ref_is_deterministic(self):
        """TASKFLOW 模式：task_ref = tf:enrich-{placeholder_id}（零通道对齐）。"""
        mux = StreamMux()
        mux.register("s1")
        cfg = DEFAULT_CONFIG.merge(enrich_mode=EnrichMode.TASKFLOW)
        mgr = EnrichManager(
            enricher=MockVectorEnricher(latency=0.01),
            mux=mux, config=cfg,
        )

        consume = asyncio.create_task(self._drain(mux))
        hit = MarkerHit(context_text="ctx")
        await mgr.on_hit("s1", "main", hit)
        await asyncio.sleep(0.1)
        await mux.force_close()
        await consume

        rec = mgr.registry.get(hit.placeholder_id)
        assert rec.task_ref == f"tf:enrich-{hit.placeholder_id}"

    async def test_local_mode_no_pending_after_settle(self):
        """local 模式：settle 超窗后无 pending（全落 failed，不画委托的饼）。"""
        mux = StreamMux()
        mux.register("s1")
        cfg = DEFAULT_CONFIG.merge(enrich_mode=EnrichMode.LOCAL)
        mgr = EnrichManager(
            enricher=MockVectorEnricher(latency=5.0),
            mux=mux, config=cfg, loading_url="loading.jpg",
        )

        consume = asyncio.create_task(self._drain(mux))
        await mgr.on_hit("s1", "main", MarkerHit(context_text="ctx"))
        await mgr.settle(timeout=0.05)
        await mux.force_close()
        await consume

        assert mgr.registry.pending() == []

    async def _drain(self, mux):
        async for _ in mux.events():
            pass


class TestResolverV032:
    """#2：resolver 修复验收。"""

    async def test_tf_namespace_deterministic_lookup(self):
        """tf: 引用按确定性 task_id 直接查询（task_id = ref 去前缀）。"""
        queried: list[str] = []

        async def tf_query(task_id):
            queried.append(task_id)
            return {"status": "succeeded", "result": {"url": "real.jpg"}}

        resolver = ManifestResolver(taskflow_query=tf_query, poll_interval=0.01)
        manifest = {"placeholders": [
            {"placeholder_id": "ph_abc", "task_ref": "tf:enrich-ph_abc", "state": "pending"},
        ]}
        results = {}

        async def cb(pid, url):
            results[pid] = url

        await resolver.resolve_all(manifest, cb)
        assert queried == ["enrich-ph_abc"]  # 确定性 ID 直查
        assert results["ph_abc"] == "real.jpg"

    async def test_local_namespace_fails_fast(self):
        """local: 残留引用立即终止（v0.3.0 会空转 max_wait=300s）。"""
        import time
        resolver = ManifestResolver(max_wait=300.0)
        manifest = {"placeholders": [
            {"placeholder_id": "ph_1", "task_ref": "local:ph_1", "state": "pending"},
        ]}
        results = {}

        async def cb(pid, url):
            results[pid] = url

        start = time.monotonic()
        await resolver.resolve_all(manifest, cb)
        assert time.monotonic() - start < 1.0
        assert results["ph_1"] is None


@pytest.mark.integration
class TestTaskflowEnricherE2E:
    """#3：TASKFLOW 模式端到端闭环（需 taskflow + Redis）。"""

    async def test_submit_poll_resolve_roundtrip(self, redis_client):
        """确定性 ID 全链：enricher 派发 → resolver 按 task_ref 轮询拿到 url。"""
        from orditect.flow import (
            BaseBackEndTask, TaskOrchestrator, get_default_storage,
        )
        from orditect.stream.adapters.taskflow import TaskflowEnricher
        from orditect.stream.protocols import EnrichRequest

        storage = get_default_storage(redis_client)
        await storage.connect()
        orchestrator = TaskOrchestrator(storage, governor=None)

        class _EnrichTask(BaseBackEndTask):
            async def execute(self, task_id: str, **kwargs):
                return {"url": f"https://oss.example.com/{task_id}.jpg"}

        enricher = TaskflowEnricher(
            orchestrator=orchestrator,
            task_factory=lambda req: _EnrichTask(storage),
            wait_timeout=10.0,
        )

        req = EnrichRequest(
            placeholder_id="ph_e2e", context_text="ctx", stream_id="s_e2e",
        )
        result = await enricher.resolve(req)
        assert result.url == "https://oss.example.com/enrich-ph_e2e.jpg"

        # resolver side polls with same convention (deterministic ID zero-channel alignment)
        resolver = ManifestResolver(
            taskflow_query=lambda tid: orchestrator.get_task(tid),
            poll_interval=0.05,
        )
        results = {}

        async def cb(pid, url):
            results[pid] = url

        await resolver.resolve_all(
            {"placeholders": [{
                "placeholder_id": "ph_e2e",
                "task_ref": "tf:enrich-ph_e2e",
                "state": "pending",
            }]},
            cb,
        )
        assert results["ph_e2e"] == "https://oss.example.com/enrich-ph_e2e.jpg"


class TestRunnerContractGuards:
    """#24/#25：契约运行时防护。"""

    async def test_run_reentry_raises(self):
        """#24：run() 重入抛 RuntimeError（一次性对象运行时防护）。"""
        class _Source:
            async def stream(self, request: SourceRequest, cancel_token=None):
                yield SourceChunk(text="x")
                yield SourceChunk(finish=True)

        runner = StreamRunner(
            stages=[StageConfig(name="m", source_type=SourceType.LLM, source=_Source())],
            enricher=MockVectorEnricher(),
            store=MemoryResultStore(),
            config=DEFAULT_CONFIG.merge(enrich_mode=EnrichMode.LOCAL),
        )
        async for _ in runner.run():
            pass

        with pytest.raises(RuntimeError, match="single-use"):
            async for _ in runner.run():
                pass

    async def test_early_consumer_exit_cascades(self):
        """#25：消费方提前 break 跳出 → 生产者被级联取消，不悬挂。"""
        started = asyncio.Event()
        finished = asyncio.Event()

        class _SlowSource:
            async def stream(self, request: SourceRequest, cancel_token=None):
                started.set()
                try:
                    for i in range(1000):
                        await asyncio.sleep(0.02)
                        yield SourceChunk(text=f"c{i}")
                    yield SourceChunk(finish=True)
                finally:
                    finished.set()  # 级联取消后源协程收尾（aclose 触发）

        runner = StreamRunner(
            stages=[StageConfig(name="m", source_type=SourceType.LLM, source=_SlowSource())],
            enricher=MockVectorEnricher(),
            store=MemoryResultStore(),
            config=DEFAULT_CONFIG.merge(enrich_mode=EnrichMode.LOCAL),
        )

        count = 0
        async for env, et in runner.run():
            count += 1
            if et == EventType.STREAM_DELTA:
                break  # 消费方提前终止（gen aclose → #25 级联）

        assert count >= 1
        # cascade effective: source coroutine finally executed (no hang)
        assert await asyncio.wait_for(finished.wait(), timeout=5.0)