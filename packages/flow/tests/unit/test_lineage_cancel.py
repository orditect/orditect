"""v0.3.0 批次 3 钉扎：contextvar 父注入 + 级联取消（R6-2 / R6-3）。

纯内存基建，无需 Redis。
"""
import asyncio
import time
from typing import Any, Dict, List, Optional

import pytest

from orditect.flow import BaseBackEndTask, TaskOrchestrator
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

    async def list_tasks(self, status=None, limit=100, offset=0):
        tasks = list(self._tasks.values())
        if status:
            tasks = [t for t in tasks if t.get("status") == status]
        return [dict(t) for t in tasks[offset:offset + limit]]


class SpyGovernor:
    def __init__(self):
        self.acquired: List[str] = []
        self.released: List[str] = []

    async def acquire(self, resource: str, timeout: Optional[float] = None) -> str:
        self.acquired.append(resource)
        return f"spy-{len(self.acquired)}"

    async def try_acquire(self, resource):
        return await self.acquire(resource)

    async def release(self, resource: str, token: str) -> None:
        self.released.append(resource)

    async def get_usage(self, resource: str) -> int:
        return 0


async def _wait_for(predicate, timeout: float = 3.0, interval: float = 0.02):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        await asyncio.sleep(interval)
    return False


class TestAutoParentInjection:
    """R6-2：嵌套 submit 自动登记谱系（业务零样板）。"""

    async def test_nested_submit_auto_registers_lineage(self):
        """父任务 execute 内 submit 子任务，parent_task_id 自动注入。"""
        storage = FakeStorage()
        orchestrator = TaskOrchestrator(storage, governor=None)
        submitted_child: list[str] = []

        class ChildTask(BaseBackEndTask):
            async def execute(self, task_id: str, **kwargs):
                return {"child": True}

        class ParentTask(BaseBackEndTask):
            async def execute(self, task_id: str, **kwargs):
                # nested submit (no explicit parent_task_id)
                child_id = await orchestrator.submit(ChildTask(storage))
                submitted_child.append(child_id)
                return {"parent": True}

        parent_id = await orchestrator.submit(ParentTask(storage))

        # wait parent and child both complete
        assert await _wait_for(
            lambda: submitted_child and
                    storage._tasks[submitted_child[0]]["status"] == "succeeded"
        )

        # lineage auto-registered
        child_record = await storage.get_task(submitted_child[0])
        assert child_record["parent_task_id"] == parent_id
        assert await storage.list_children(parent_id) == [submitted_child[0]]

    async def test_explicit_parent_wins_over_context(self):
        """显式传 parent_task_id 优先于上下文自动注入。"""
        storage = FakeStorage()
        orchestrator = TaskOrchestrator(storage, governor=None)
        submitted_child: list[str] = []

        class ChildTask(BaseBackEndTask):
            async def execute(self, task_id: str, **kwargs):
                return {"child": True}

        class ParentTask(BaseBackEndTask):
            async def execute(self, task_id: str, **kwargs):
                child_id = await orchestrator.submit(
                    ChildTask(storage), parent_task_id="explicit_parent"
                )
                submitted_child.append(child_id)
                return {"parent": True}

        parent_id = await orchestrator.submit(ParentTask(storage))
        assert await _wait_for(lambda: bool(submitted_child))

        child_record = await storage.get_task(submitted_child[0])
        assert child_record["parent_task_id"] == "explicit_parent"  # 不是 parent_id

    async def test_top_level_submit_is_root(self):
        """顶层 submit（无执行上下文）为根任务，无 parent。"""
        storage = FakeStorage()
        orchestrator = TaskOrchestrator(storage, governor=None)

        class SimpleTask(BaseBackEndTask):
            async def execute(self, task_id: str, **kwargs):
                return {"ok": True}

        task_id = await orchestrator.submit(SimpleTask(storage))
        record = await storage.get_task(task_id)
        assert "parent_task_id" not in record  # 根任务无谱系字段


class TestCascadeCancel:
    """R6-3：cancel 沿谱系级联。"""

    async def test_cancel_parent_cascades_to_children(self):
        """cancel 父任务 → 子任务、孙任务全部级联取消。"""
        storage = FakeStorage()
        orchestrator = TaskOrchestrator(storage, governor=None)

        # manually construct three-layer lineage (no execute, directly test cascade logic)
        await storage.initialize_task("grandparent", "running")
        await storage.initialize_task("parent", "running", parent_task_id="grandparent")
        await storage.initialize_task("child", "queued", parent_task_id="parent")
        await storage.initialize_task("unrelated", "running")

        ok = await orchestrator.cancel("grandparent")
        assert ok is True

        # all three cancelled
        for tid in ("grandparent", "parent", "child"):
            stored = await storage.get_task(tid)
            assert stored["status"] == "cancelled", f"{tid} should be cancelled"
            assert stored["cancel_requested"] is True

        # unrelated not affected
        unrelated = await storage.get_task("unrelated")
        assert unrelated["status"] == "running"

    async def test_cascade_skips_terminal_children(self):
        """级联时终态子任务跳过。"""
        storage = FakeStorage()
        orchestrator = TaskOrchestrator(storage, governor=None)

        await storage.initialize_task("parent", "running")
        await storage.initialize_task("done_child", "succeeded", parent_task_id="parent")
        await storage.initialize_task("live_child", "running", parent_task_id="parent")

        await orchestrator.cancel("parent")

        done = await storage.get_task("done_child")
        assert done["status"] == "succeeded"  # 终态不动

        live = await storage.get_task("live_child")
        assert live["status"] == "cancelled"

    async def test_cascade_depth_limit_no_infinite_loop(self):
        """谱系成环时深度上限防死循环。"""
        storage = FakeStorage()
        orchestrator = TaskOrchestrator(storage, governor=None)

        # A → B → A cycle (abnormal data, defense target)
        await storage.initialize_task("a", "running")
        await storage.initialize_task("b", "running", parent_task_id="a")
        # manually set a's parent to b (cycle)
        await storage.update_task("a", {"parent_task_id": "b"})

        # not hang means pass (depth limit effective)
        await asyncio.wait_for(orchestrator.cancel("a"), timeout=5.0)


class TestCascadeTerminate:
    """R6-3：terminate 沿谱系级联（含协程通道）。"""

    async def test_terminate_parent_kills_child_coroutine(self):
        """terminate 父 → 本进程运行的子协程被取消、资源释放。"""
        storage = FakeStorage()
        spy = SpyGovernor()
        orchestrator = TaskOrchestrator(storage, spy)
        child_started = asyncio.Event()

        class LongChildTask(BaseBackEndTask):
            async def execute(self, task_id: str, **kwargs):
                child_started.set()
                for _ in range(200):
                    await asyncio.sleep(0.05)
                return {"done": True}

        class ParentTask(BaseBackEndTask):
            async def execute(self, task_id: str, **kwargs):
                await orchestrator.submit(
                    LongChildTask(storage), task_id="child_task"
                )
                for _ in range(200):
                    await asyncio.sleep(0.05)
                return {"parent": True}

        parent_id = await orchestrator.submit(ParentTask(storage))

        # wait child coroutine start
        assert await asyncio.wait_for(child_started.wait(), timeout=3.0)

        ok = await orchestrator.terminate(parent_id)
        assert ok is True

        # wait cascade finish
        assert await _wait_for(
            lambda: storage._tasks.get("child_task", {}).get("status") == "cancelled"
        )

        # child coroutine cancelled, resource released
        child = await storage.get_task("child_task")
        assert child["status"] == "cancelled"
        assert "task_execution" in spy.released