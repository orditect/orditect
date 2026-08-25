"""任务生命周期集成测试"""
import pytest
import asyncio

from orditect.flow import (
    BaseBackEndTask,
    TaskOrchestrator,
    TaskStatus,
    get_default_storage,
    UnlimitedGovernor,
)


class SimpleTask(BaseBackEndTask):
    """简单任务"""

    async def execute(self, task_id: str, **kwargs):
        await asyncio.sleep(0.1)
        return {"result": "success"}


@pytest.mark.integration
class TestTaskLifecycle:
    """任务生命周期测试"""

    async def test_submit_and_complete(self, redis_client):
        """提交并完成"""
        storage = get_default_storage(redis_client)
        if hasattr(storage, 'connect'):
            await storage.connect()

        governor = UnlimitedGovernor()
        orchestrator = TaskOrchestrator(storage, governor)

        task = SimpleTask(storage, governor)
        task_id = await orchestrator.submit(task)

        await asyncio.sleep(0.5)

        status = await orchestrator.get_status(task_id)
        assert status == TaskStatus.SUCCEEDED

        task_data = await orchestrator.get_task(task_id)
        assert task_data["result"] == {"result": "success"}

        # v0.3.3: drain (real Redis coroutine lives longer, not drained will be reported by GC in later tests)
        await orchestrator.wait_all_finalized()

    async def test_cancel_task(self, redis_client):
        """取消任务"""
        storage = get_default_storage(redis_client)
        if hasattr(storage, 'connect'):
            await storage.connect()

        governor = UnlimitedGovernor()
        orchestrator = TaskOrchestrator(storage, governor)

        class LongTask(BaseBackEndTask):
            async def execute(self, task_id: str, **kwargs):
                await asyncio.sleep(10)
                return {"result": "success"}

        task = LongTask(storage, governor)
        task_id = await orchestrator.submit(task)

        await asyncio.sleep(0.1)
        success = await orchestrator.cancel(task_id)
        assert success is True

        status = await orchestrator.get_status(task_id)
        assert status == TaskStatus.CANCELLED

        # v0.3.3: drain
        await orchestrator.wait_all_finalized()