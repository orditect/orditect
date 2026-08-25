"""StreamRunner cancel(force) + EnrichManager.cancel_placeholder 单元测试（P2 可选优化项）。

覆盖：
- P2-1 强制 cancel：cancel(force=True) 取消 executor 协程，sem 立即释放
  （对比默认模式：sem 持有至 LLM 真实结束）
- P2-1 真实链路：force cancel + 客户端断开（watcher 路径）→ 级联取消 → 流终止干净
- P2-2 单个 enrich cancel：cancel_placeholder 成功/幂等/立即失败状态落定
- P2-3 资源清理验证：cancel 后 governor.get_usage() 断言水位归零（作为测试
  用例实现，非框架 API）

测试基建为纯内存（TrackingGovernor 直接读内部状态，不残留 token），无需 Redis。
"""
import asyncio

import pytest

from orditect.stream.config import DEFAULT_CONFIG, EnrichMode
from orditect.stream.core import CancellationToken
from orditect.stream.enrich import EnrichManager, MockVectorEnricher
from orditect.stream.events import EventType, PlaceholderState
from orditect.stream.mux import StreamMux
from orditect.stream.pipeline import MarkerHit
from orditect.stream.runner import StreamRunner
from orditect.stream.stages import (
    DEFAULT_STREAM_LLM_RESOURCE,
    SourceType,
    StageConfig,
)
from orditect.stream.store import MemoryResultStore
from orditect.stream.protocols import SourceChunk, SourceRequest


# ---------- test infrastructure ----------

class TrackingGovernor:
    """内存 governor：记录 acquire/release，支持 usage 查询（资源清理验证）。"""

    def __init__(self):
        self.tokens: dict[str, str] = {}          # token -> resource
        self.acquired_log: list[str] = []
        self.released_log: list[str] = []

    async def acquire(self, resource: str, timeout: float | None = None) -> str:
        token = f"track-token-{len(self.tokens)}-{len(self.acquired_log)}"
        self.tokens[token] = resource
        self.acquired_log.append(resource)
        return token

    async def try_acquire(self, resource: str) -> str | None:
        return await self.acquire(resource)

    async def release(self, resource: str, token: str) -> None:
        if token in self.tokens:
            del self.tokens[token]
            self.released_log.append(resource)

    async def get_usage(self, resource: str) -> int:
        return sum(1 for r in self.tokens.values() if r == resource)


class _SlowSource:
    """慢速 LLM 源：每个 chunk 间隔 delay，可中途取消。"""

    def __init__(self, chunks, delay: float = 0.05):
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


def _make_runner(governor, chunks, delay=0.05, **kwargs):
    cfg = DEFAULT_CONFIG.merge(enrich_mode=EnrichMode.LOCAL)
    return StreamRunner(
        stages=[
            StageConfig(
                name="main",
                source_type=SourceType.LLM,
                source=_SlowSource(chunks, delay=delay),
                resource=DEFAULT_STREAM_LLM_RESOURCE,
            ),
        ],
        enricher=MockVectorEnricher(),
        store=MemoryResultStore(),
        config=cfg,
        governor=governor,
        **kwargs,
    )


_LONG_CHUNKS = [SourceChunk(text=f"第{i}段") for i in range(10)] + [SourceChunk(finish=True)]


# ---------- P2-1: force cancel ----------

class TestStreamRunnerForceCancel:
    async def test_force_cancel_releases_sem_immediately(self):
        """force=True：协程被取消，sem 立即释放（不等待 LLM 消费完）。"""
        governor = TrackingGovernor()
        runner = _make_runner(governor, _LONG_CHUNKS, delay=0.1)  # 全程 ~1s

        events_task = asyncio.create_task(_collect(runner))
        await asyncio.sleep(0.2)  # 确保已 acquire 且远未结束

        assert await governor.get_usage(DEFAULT_STREAM_LLM_RESOURCE) == 1

        stream_id = runner._stream_ids[0]
        await runner.cancel(stream_id=stream_id, reason="force_test", force=True)

        # wait executor coroutine finish (sem released in StageRunner finally)
        deadline = asyncio.get_running_loop().time() + 2.0
        while runner._executor_tasks and not runner._executor_tasks[0].done():
            if asyncio.get_running_loop().time() > deadline:
                pytest.fail("executor task did not finish after force cancel")
            await asyncio.sleep(0.02)

        # P2-3 resource cleanup verification: sem released (LLM far from fully consumed, proves force release)
        assert await governor.get_usage(DEFAULT_STREAM_LLM_RESOURCE) == 0
        assert governor.released_log == [DEFAULT_STREAM_LLM_RESOURCE]

        await events_task  # 流完整结束

    async def test_default_cancel_holds_sem_until_llm_done(self):
        """对比组：force=False（默认）sem 持有至 LLM 真实结束。"""
        governor = TrackingGovernor()
        runner = _make_runner(governor, _LONG_CHUNKS, delay=0.03)  # 全程 ~0.3s

        events_task = asyncio.create_task(_collect(runner))
        await asyncio.sleep(0.1)  # 流运行中

        stream_id = runner._stream_ids[0]
        await runner.cancel(stream_id=stream_id, reason="graceful", force=False)

        # after cancel LLM still consuming, sem still held
        await asyncio.sleep(0.05)
        assert await governor.get_usage(DEFAULT_STREAM_LLM_RESOURCE) == 1

        # sem released after stream ends
        await events_task
        assert await governor.get_usage(DEFAULT_STREAM_LLM_RESOURCE) == 0

    async def test_force_cancel_with_disconnect_completes_lifecycle(self):
        """真实场景复现：force cancel + 客户端断开（watcher 路径）。

        FastAPI 真实链路：cancel(force=True) 后客户端断开 → notify_disconnect()
        → grace 超时 → _on_cancel() 级联 → producer 收尾关闭子流。
        验证三件事：
        1. cancelled 事件已下发（在 disconnect 缓冲生效前完成下发）
        2. sem 已立即释放（force 生效）
        3. 流正常终止（producer 收尾干净，无泄漏/无悬挂）
        """
        cfg = DEFAULT_CONFIG.merge(
            enrich_mode=EnrichMode.LOCAL,
            grace_period=0.05,  # 短宽限期，快速触发级联取消
        )
        governor = TrackingGovernor()
        runner = StreamRunner(
            stages=[
                StageConfig(
                    name="main",
                    source_type=SourceType.LLM,
                    source=_SlowSource(_LONG_CHUNKS, delay=0.1),
                    resource=DEFAULT_STREAM_LLM_RESOURCE,
                ),
            ],
            enricher=MockVectorEnricher(),
            store=MemoryResultStore(),
            config=cfg,
            governor=governor,
        )

        events_task = asyncio.create_task(_collect(runner))
        await asyncio.sleep(0.2)  # 确保已 acquire 且远未结束

        # force cancel (immediately release sem)
        stream_id = runner._stream_ids[0]
        await runner.cancel(stream_id=stream_id, reason="force_test", force=True)

        # let cancelled event be consumed by loop first (event enqueue→consume needs event loop tick;
        # real scenario has natural network round-trip between cancel response and client disconnect)
        await asyncio.sleep(0.02)

        # client disconnects (real scenario triggered by create_stream_response watcher)
        await runner.notify_disconnect()

        events = await events_task  # grace 超时 → 级联取消 → producer 收尾 → 流终止
        types = [et for _, et in events]

        # 1. cancelled event delivered (before disconnect buffering takes effect)
        assert EventType.STREAM_CANCELLED in types

        # 2. sem already released (force effective, not waiting LLM consumption)
        assert await governor.get_usage(DEFAULT_STREAM_LLM_RESOURCE) == 0

        # 3. stream terminates normally (events_task returns proves producer cleaned up):
        # if producer hangs/leaks, await would timeout or pytest reports unretrieved task
        executor_done = all(t.done() for t in runner._executor_tasks)
        assert executor_done

    async def test_force_cancel_all_streams(self):
        """force=True + stream_id=None：所有子流协程被取消。"""
        governor = TrackingGovernor()
        runner = _make_runner(governor, _LONG_CHUNKS, delay=0.1, max_id=2)

        events_task = asyncio.create_task(_collect(runner))
        await asyncio.sleep(0.2)

        await runner.cancel(reason="force_all", force=True)

        events = await events_task
        cancel_events = [e for e, et in events if et == EventType.STREAM_CANCELLED]
        assert len(cancel_events) == 2

        # P2-3 resource cleanup verification: both sems released
        assert await governor.get_usage(DEFAULT_STREAM_LLM_RESOURCE) == 0


# ---------- P2-2: single enrich cancel ----------

class TestEnrichManagerCancelPlaceholder:
    async def _setup_manager(self, latency=0.5):
        mux = StreamMux()
        mux.register("s1")
        cfg = DEFAULT_CONFIG.merge(enrich_mode=EnrichMode.LOCAL)
        manager = EnrichManager(
            enricher=MockVectorEnricher(latency=latency),
            mux=mux,
            config=cfg,
        )
        return mux, manager

    async def test_cancel_placeholder_success(self):
        """取消单个 enrich：返回 True，状态落 failed，settle 立即放行。"""
        mux, manager = await self._setup_manager(latency=0.5)
        hit = MarkerHit(context_text="正文段落")
        await manager.on_hit("s1", "main", hit)
        await asyncio.sleep(0.05)  # 确保任务已启动

        ok = await manager.cancel_placeholder(hit.placeholder_id)
        assert ok is True

        # status settled (cancel_placeholder waits mark_failed completion before returning)
        rec = manager.registry.get(hit.placeholder_id)
        assert rec.state is PlaceholderState.FAILED
        assert "cancelled" in rec.error

        # settle releases immediately (record non-pending)
        import time
        start = time.monotonic()
        await manager.settle(timeout=5.0)
        assert time.monotonic() - start < 1.0

    async def test_cancel_placeholder_idempotent(self):
        """不存在/已完成：返回 False。"""
        mux, manager = await self._setup_manager(latency=0.01)

        # nonexistent id
        assert await manager.cancel_placeholder("ph_nonexistent") is False

        # already completed (task ended after resolve)
        hit = MarkerHit(context_text="正文段落")
        await manager.on_hit("s1", "main", hit)
        await asyncio.sleep(0.1)  # 等 mock 完成
        assert await manager.cancel_placeholder(hit.placeholder_id) is False

    async def test_cancel_all_marks_failed_fast_settle(self):
        """cancel_all 后 record 落 failed（CancelledError 分支），settle 不空等。"""
        mux, manager = await self._setup_manager(latency=5.0)  # 超长延迟
        hit = MarkerHit(context_text="正文段落")
        await manager.on_hit("s1", "main", hit)
        await asyncio.sleep(0.05)

        await manager.cancel_all()

        # post-fix behavior: record falls failed (pre-fix stuck pending, settle empty wait timeout)
        rec = manager.registry.get(hit.placeholder_id)
        assert rec.state is PlaceholderState.FAILED

        import time
        start = time.monotonic()
        await manager.settle(timeout=5.0)
        assert time.monotonic() - start < 1.0