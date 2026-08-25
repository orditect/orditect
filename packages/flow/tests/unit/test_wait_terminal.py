"""wait_terminal 工具方法钉扎（v0.3.0 批次 4-1）。"""
import asyncio
import time
from typing import Any, Dict, List, Optional

import pytest

from orditect.flow import (
    BaseBackEndTask,
    TaskOrchestrator,
    TaskStatus,
)
from orditect.flow.exceptions import TaskNotFoundError


class FakeStorage:
    def __init__(self):
        self._tasks: Dict[str, Dict[str, Any]] = {}

    async def initialize_task(
        self, task_id: str, initial_status: str, *,
        parent_task_id: Optional[str] = None, if_not_exists: bool = False,
    ) -> bool:
        if if_not_exists and task_id in self._tasks:
            return False
        self._tasks[task_id] = {
            "task_id": task_id, "status": initial_status,
            "progress": 0.0, "cancel_requested": False,
        }
        if parent_task_id is not None:
            self._tasks[task_id]["parent_task_id"] = parent_task_id
        return True

    async def list_children(self, parent_task_id: str) -> List[str]:
        return [tid for tid, t in self._tasks.items()
                if t.get("parent_task_id") == parent_task_id]

    async def update_task(self, task_id, updates, **kwargs):
        if task_id not in self._tasks:
            raise TaskNotFoundError(task_id)
        self._tasks[task_id].update(updates)

    async def get_task(self, task_id):
        if task_id not in self._tasks:
            raise TaskNotFoundError(task_id)
        return dict(self._tasks[task_id])

    async def request_cancel(self, task_id):
        if task_id not in self._tasks:
            return False
        self._tasks[task_id]["cancel_requested"] = True
        return True

    async def list_task_ids_by_status(self, status: str) -> List[str]:
        return [tid for tid, t in self._tasks.items()
                if t.get("status") == status]

    async def bulk_get_tasks(self, task_ids):
        return [dict(self._tasks.get(tid, {})) for tid in task_ids]


class QuickTask(BaseBackEndTask):
    def __init__(self, storage, governor=None, sleep: float = 0.1):
        super().__init__(storage, governor)
        self._sleep = sleep

    async def execute(self, task_id: str, **kwargs):
        await asyncio.sleep(self._sleep)
        return {"ok": True}


class TestWaitTerminal:
    async def test_wait_until_succeeded(self):
        """任务完成后返回完整记录（含 result）。"""
        storage = FakeStorage()
        orchestrator = TaskOrchestrator(storage, governor=None)

        task_id = await orchestrator.submit(QuickTask(storage, sleep=0.1))

        record = await orchestrator.wait_terminal(task_id, timeout=3.0)
        assert record["status"] == "succeeded"
        assert record["result"] == {"ok": True}
        assert record["task_id"] == task_id

    async def test_timeout_raises(self):
        """超时未终态 → 抛内置 TimeoutError。"""
        storage = FakeStorage()
        orchestrator = TaskOrchestrator(storage, governor=None)

        # manually place a task that never reaches terminal (no submit, no coroutine)
        await storage.initialize_task("stuck", "running")

        start = time.monotonic()
        with pytest.raises(TimeoutError, match="wait_terminal timeout"):
            await orchestrator.wait_terminal("stuck", timeout=0.3)
        elapsed = time.monotonic() - start
        assert elapsed < 1.0  # 超时确实生效（非挂死）

    async def test_not_found_raises(self):
        """任务不存在 → TaskNotFoundError 原样传播。"""
        storage = FakeStorage()
        orchestrator = TaskOrchestrator(storage, governor=None)

        with pytest.raises(TaskNotFoundError):
            await orchestrator.wait_terminal("ghost", timeout=0.5)

    async def test_already_terminal_returns_immediately(self):
        """已终态任务：立即返回（不等满 timeout）。"""
        storage = FakeStorage()
        orchestrator = TaskOrchestrator(storage, governor=None)

        await storage.initialize_task("done", "succeeded")

        start = time.monotonic()
        record = await orchestrator.wait_terminal("done", timeout=30.0)
        elapsed = time.monotonic() - start

        assert record["status"] == "succeeded"
        assert elapsed < 0.5  # 立即返回

    async def test_nested_submit_waits_child(self):
        """递归场景：父任务内 wait_terminal 等子任务（端到端）。"""
        storage = FakeStorage()
        orchestrator = TaskOrchestrator(storage, governor=None)

        class ChildTask(BaseBackEndTask):
            async def execute(self, task_id: str, **kwargs):
                await asyncio.sleep(0.05)
                return {"child": 42}

        class ParentTask(BaseBackEndTask):
            async def execute(self, task_id: str, **kwargs):
                child_id = await orchestrator.submit(ChildTask(storage))
                child = await orchestrator.wait_terminal(child_id, timeout=3.0)
                return {"got": child["result"]["child"]}

        parent_id = await orchestrator.submit(ParentTask(storage))
        record = await orchestrator.wait_terminal(parent_id, timeout=3.0)

        assert record["status"] == "succeeded"
        assert record["result"] == {"got": 42}