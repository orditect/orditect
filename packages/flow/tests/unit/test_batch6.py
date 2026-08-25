"""v0.3.0 批次 6 钉扎：held 失真修复 + R13 契约显式化。"""
import asyncio

import pytest

from orditect.flow import (
    DelayedScheduler,
    DeadLetterQueue,
    TaskOrchestrator,
    TaskStatus,
    UnlimitedGovernor,
    get_default_storage,
)
from orditect.flow.governor.factory import TaskbaseGovernorAdapter


class TestHeldAccuracy:
    """held 失真修复验收（adapter token 映射）。"""

    @pytest.mark.integration
    async def test_held_duration_accurate(self, redis_client):
        """release 时还原真实 acquired_at（修复前：held ≈ 0）。"""
        from orditect.core import get_registry
        registry = get_registry()
        registry.clear()

        held_records = []

        class SpyHooks:
            async def on_released(self, resource: str, held: float):
                held_records.append(held)

        sem = registry.register_semaphore(
            "test_held", redis_client, limit=5, lease_time=30.0,
            hooks=[SpyHooks()],
        )

        adapter = TaskbaseGovernorAdapter(registry)
        token = await adapter.acquire("test_held")

        await asyncio.sleep(0.2)  # 持有 0.2s

        await adapter.release("test_held", token)

        assert len(held_records) == 1
        # pre-fix held ≈ 0 (time.monotonic() approximation); post-fix should be ≥ 0.15
        assert held_records[0] >= 0.15, f"held duration inaccurate: {held_records[0]}"

        registry.clear()

    @pytest.mark.integration
    async def test_release_unknown_token_still_works(self, redis_client):
        """映射缺失时 release 功能正常（held 失真但不炸）。"""
        from orditect.core import get_registry
        registry = get_registry()
        registry.clear()
        registry.register_semaphore("test_unknown", redis_client, limit=5, lease_time=30.0)

        adapter = TaskbaseGovernorAdapter(registry)
        token = await adapter.acquire("test_unknown")

        # manually clear mapping (simulate adapter restart releasing old token)
        adapter._tokens.clear()

        # should not throw exception
        await adapter.release("test_unknown", token)
        registry.clear()


class TestR13ExplicitContracts:
    """R13 契约显式化验收。"""

    async def test_delayed_scheduler_get_ready_always_empty(self):
        """DelayedScheduler.get_ready_tasks 永远返回空（骨架显式化）。"""
        scheduler = DelayedScheduler()
        await scheduler.schedule("t1", delay=0.01)
        await asyncio.sleep(0.05)
        assert await scheduler.get_ready_tasks() == []

    async def test_dlq_retry_logs_skeleton_warning(self, redis_client, caplog):
        """DLQ.retry 明确告警"未真正重执行"。"""
        dlq = DeadLetterQueue(redis_client, key_prefix="test_dlq_b6")

        async def dummy():
            pass

        task_id = await dlq.add(
            func=dummy, args=(), kwargs={}, error=Exception("test"),
        )

        import logging
        with caplog.at_level(logging.WARNING):
            await dlq.retry(task_id)

        assert any("NOT re-executed" in r.message or "skeleton" in r.message
                   for r in caplog.records), "DLQ retry should warn about skeleton behavior"

        # task already deleted from DLQ (contract unchanged)
        assert await dlq.get(task_id) is None


class TestListTasksOrchestratorLevel:
    """list_tasks 归属编排层验收。"""

    @pytest.mark.integration
    async def test_orchestrator_list_tasks(self, redis_client):
        """orchestrator.list_tasks 组合原语正常工作。"""
        storage = get_default_storage(redis_client)
        if hasattr(storage, "connect"):
            await storage.connect()

        orchestrator = TaskOrchestrator(storage, UnlimitedGovernor())

        from orditect.flow import BaseBackEndTask

        class QuickTask(BaseBackEndTask):
            async def execute(self, task_id: str, **kwargs):
                return {"ok": True}

        task_id = await orchestrator.submit(QuickTask(storage))

        import time
        deadline = time.monotonic() + 3.0
        while time.monotonic() < deadline:
            status = await orchestrator.get_status(task_id)
            if status == TaskStatus.SUCCEEDED:
                break
            await asyncio.sleep(0.02)

        succeeded = await orchestrator.list_tasks(status=TaskStatus.SUCCEEDED)
        assert any(t["task_id"] == task_id for t in succeeded)

    async def test_list_tasks_no_status_raises(self):
        """无 status 过滤显式拒绝（反模式防御）。"""
        storage = None  # 不需要真 storage，ValueError 在 storage 调用前抛出
        orchestrator = TaskOrchestrator.__new__(TaskOrchestrator)
        orchestrator.storage = storage

        with pytest.raises(ValueError, match="explicit status filter"):
            await orchestrator.list_tasks(status=None)