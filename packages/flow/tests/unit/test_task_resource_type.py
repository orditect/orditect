"""多类型治理单元测试（升级清单任务 1）。

覆盖：
- BaseBackEndTask.resource_type 类属性（默认值 / 子类覆盖）
- TaskExecutor 资源名解析：resource or task.resource_type
- TaskExecutor 向任务 kwargs 注入 governor（双层治理之注入点）
- TaskOrchestrator.submit 端到端资源类型路由（含显式 resource 向后兼容）

测试基建：
- FakeStorage：内存任务存储（update_task 带 **kwargs，
  兼容 executor 的 validate_status_transfer=False 调用）
- SpyGovernor：记录 acquire/release 调用的资源名
全部为纯内存测试，无需 Redis。
"""
import asyncio
import time
from typing import Any, Dict, List, Optional

import pytest

from orditect.flow import (
    BaseBackEndTask,
    TaskExecutor,
    TaskOrchestrator,
)
from orditect.flow.exceptions import TaskNotFoundError


# ---------- test infrastructure ----------

class FakeStorage:
    """内存任务存储（实现 TaskStorageProtocol）。

    update_task 带 **kwargs：executor/lifecycle 调用时会传
    validate_status_transfer=False，协议未声明该参数，
    这里用 **kwargs 兼容（与 taskbase TaskRedisDB 的实际签名对齐）。
    """

    def __init__(self):
        self._tasks: Dict[str, Dict[str, Any]] = {}

    async def initialize_task(
        self,
        task_id: str,
        initial_status: str,
        *,
        parent_task_id: Optional[str] = None,
        if_not_exists: bool = False,
    ) -> bool:
        if if_not_exists and task_id in self._tasks:
            return False
        self._tasks[task_id] = {
            "task_id": task_id,
            "status": initial_status,
            "progress": 0.0,
            "cancel_requested": False,
        }
        if parent_task_id is not None:
            self._tasks[task_id]["parent_task_id"] = parent_task_id
        return True

    async def list_children(self, parent_task_id: str) -> List[str]:
        return [
            tid for tid, t in self._tasks.items()
            if t.get("parent_task_id") == parent_task_id
        ]

    async def update_task(self, task_id: str, updates: Dict[str, Any], **kwargs) -> None:
        if task_id not in self._tasks:
            raise TaskNotFoundError(f"task_id not found: {task_id}")
        self._tasks[task_id].update(updates)

    async def get_task(self, task_id: str) -> Dict[str, Any]:
        if task_id not in self._tasks:
            raise TaskNotFoundError(f"task_id not found: {task_id}")
        return dict(self._tasks[task_id])

    async def request_cancel(self, task_id: str) -> bool:
        if task_id not in self._tasks:
            return False
        self._tasks[task_id]["cancel_requested"] = True
        return True

    async def list_tasks(
        self,
        status: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        tasks = list(self._tasks.values())
        if status:
            tasks = [t for t in tasks if t.get("status") == status]
        return [dict(t) for t in tasks[offset:offset + limit]]


class SpyGovernor:
    """测试治理：记录 acquire/release 调用的资源名。"""

    def __init__(self):
        self.acquired: List[str] = []
        self.released: List[str] = []
        self._counter = 0

    async def acquire(self, resource: str, timeout: Optional[float] = None) -> str:
        self.acquired.append(resource)
        self._counter += 1
        return f"spy-token-{self._counter}"

    async def try_acquire(self, resource: str) -> Optional[str]:
        return await self.acquire(resource)

    async def release(self, resource: str, token: str) -> None:
        self.released.append(resource)

    async def get_usage(self, resource: str) -> int:
        return 0


class RecordingTask(BaseBackEndTask):
    """记录 execute 收到的 kwargs 的任务。"""

    async def execute(self, task_id: str, **kwargs):
        self.received_kwargs = kwargs
        return {"ok": True}


class AgentTask(RecordingTask):
    """子类覆盖资源类型。"""

    resource_type = "task_agent"


async def _wait_terminal(storage: FakeStorage, task_id: str, timeout: float = 2.0) -> dict:
    """轮询等待任务到达终态（submit 后台执行场景）。"""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        task = await storage.get_task(task_id)
        if task["status"] in ("succeeded", "failed", "cancelled"):
            return task
        await asyncio.sleep(0.02)
    raise AssertionError(f"task {task_id} did not reach terminal state in {timeout}s")


# ---------- test cases ----------

class TestResourceTypeAttribute:
    """resource_type 类属性测试。"""

    def test_default_resource_type(self):
        """默认资源类型为 task_execution。"""
        assert BaseBackEndTask.resource_type == "task_execution"

        storage = FakeStorage()
        task = RecordingTask(storage)
        assert task.resource_type == "task_execution"

    def test_subclass_override(self):
        """子类可覆盖 resource_type。"""
        storage = FakeStorage()
        task = AgentTask(storage)
        assert task.resource_type == "task_agent"
        # class attribute override does not affect base class
        assert BaseBackEndTask.resource_type == "task_execution"


class TestExecutorResourceResolution:
    """TaskExecutor 资源名解析测试（resource or task.resource_type）。"""

    async def test_executor_uses_task_resource_type(self):
        """resource=None 时使用任务的 resource_type。"""
        storage = FakeStorage()
        spy = SpyGovernor()
        executor = TaskExecutor(storage, spy)

        await storage.initialize_task("t1", "pending")
        task = AgentTask(storage, spy)

        await executor.execute(task_id="t1", task=task)

        assert spy.acquired == ["task_agent"]
        assert spy.released == ["task_agent"]

    async def test_executor_explicit_resource_wins(self):
        """显式 resource 优先于 resource_type。"""
        storage = FakeStorage()
        spy = SpyGovernor()
        executor = TaskExecutor(storage, spy)

        await storage.initialize_task("t1", "pending")
        task = AgentTask(storage, spy)

        await executor.execute(task_id="t1", task=task, resource="explicit_pool")

        assert spy.acquired == ["explicit_pool"]
        assert spy.released == ["explicit_pool"]

    async def test_executor_default_fallback(self):
        """未覆盖 resource_type 且 resource=None 时回退 task_execution。"""
        storage = FakeStorage()
        spy = SpyGovernor()
        executor = TaskExecutor(storage, spy)

        await storage.initialize_task("t1", "pending")
        task = RecordingTask(storage, spy)

        await executor.execute(task_id="t1", task=task)

        assert spy.acquired == ["task_execution"]


class TestGovernorInjection:
    """TaskExecutor 向任务注入 governor 测试（双层治理注入点）。"""

    async def test_governor_injected_into_kwargs(self):
        """任务可通过 kwargs.get('governor') 获取 executor 的 governor。"""
        storage = FakeStorage()
        spy = SpyGovernor()
        executor = TaskExecutor(storage, spy)

        await storage.initialize_task("t1", "pending")
        task = RecordingTask(storage, spy)

        await executor.execute(task_id="t1", task=task)

        assert task.received_kwargs["governor"] is spy

    async def test_explicit_governor_kwarg_not_overwritten(self):
        """调用方显式传入 governor 时不被 setdefault 覆盖。"""
        storage = FakeStorage()
        spy = SpyGovernor()
        other = SpyGovernor()
        executor = TaskExecutor(storage, spy)

        await storage.initialize_task("t1", "pending")
        task = RecordingTask(storage, spy)

        await executor.execute(task_id="t1", task=task, governor=other)

        assert task.received_kwargs["governor"] is other

    async def test_none_governor_no_acquire(self):
        """executor 无 governor 时不获取令牌，注入值为 None。"""
        storage = FakeStorage()
        executor = TaskExecutor(storage, governor=None)

        await storage.initialize_task("t1", "pending")
        task = RecordingTask(storage, governor=None)

        await executor.execute(task_id="t1", task=task)

        assert task.received_kwargs["governor"] is None


class TestOrchestratorResourceType:
    """TaskOrchestrator.submit 端到端资源类型路由。"""

    async def test_submit_uses_resource_type(self):
        """submit 不传 resource 时按任务 resource_type 治理。"""
        storage = FakeStorage()
        spy = SpyGovernor()
        orchestrator = TaskOrchestrator(storage, spy)

        task = AgentTask(storage, spy)
        task_id = await orchestrator.submit(task)

        final = await _wait_terminal(storage, task_id)
        assert final["status"] == "succeeded"
        assert spy.acquired == ["task_agent"]
        assert spy.released == ["task_agent"]

    async def test_submit_explicit_resource_backward_compat(self):
        """submit 显式传 resource 时保持旧行为（覆盖 resource_type）。"""
        storage = FakeStorage()
        spy = SpyGovernor()
        orchestrator = TaskOrchestrator(storage, spy)

        task = AgentTask(storage, spy)
        task_id = await orchestrator.submit(task, resource="llm")

        final = await _wait_terminal(storage, task_id)
        assert final["status"] == "succeeded"
        assert spy.acquired == ["llm"]