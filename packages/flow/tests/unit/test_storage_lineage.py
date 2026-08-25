"""v0.3.0 批次 2 钉扎：协议扩展（谱系 + 幂等）实现对齐。

v0.3.0 批次 5 后：本地存储已删除，taskbase 升格硬依赖。本文件保留：
- FakeStorage：协议基准（纯内存，unit）
- taskbase 矩阵：真实存储验收（integration，需 Redis，未安装自动 skip）

词表纪律：taskbase 有真实状态机校验（taskflow 词表 pending→running 不直通），
涉及状态变更的断言用 validate_status_transfer=False 绕过——
本文件钉的是"谱系 + 幂等"语义，不是词表校验（词表在 test_v030_consistency 钉）。
"""
from typing import Any, Dict, List, Optional

import pytest

from orditect.flow import get_default_storage
from orditect.flow.exceptions import TaskNotFoundError

try:
    import orditect.core  # noqa
    HAS_TASKBASE = True
except ImportError:
    HAS_TASKBASE = False

requires_taskbase = pytest.mark.skipif(not HAS_TASKBASE, reason="orditect-core not installed")


class FakeStorage:
    """内存实现（协议对齐基准）。"""

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


async def _lineage_matrix_asserts(storage):
    """谱系 + 幂等断言集（协议基准用，无状态机校验的实现）。"""
    # lineage registration
    assert await storage.initialize_task("parent", "pending") is True
    assert await storage.initialize_task("child_1", "pending", parent_task_id="parent") is True
    assert await storage.initialize_task("child_2", "pending", parent_task_id="parent") is True
    assert await storage.initialize_task("orphan", "pending") is True

    children = await storage.list_children("parent")
    assert set(children) == {"child_1", "child_2"}
    assert await storage.list_children("orphan") == []

    # record carries lineage fields
    child = await storage.get_task("child_1")
    assert child["parent_task_id"] == "parent"

    # idempotent
    assert await storage.initialize_task("child_1", "pending", if_not_exists=True) is False
    # after idempotent skip status not wiped
    await storage.update_task("child_1", {"status": "running"})
    assert await storage.initialize_task("child_1", "pending", if_not_exists=True) is False
    child = await storage.get_task("child_1")
    assert child["status"] == "running"

    # default behavior (if_not_exists=False) keeps overwrite semantics
    assert await storage.initialize_task("child_2", "pending") is True
    child2 = await storage.get_task("child_2")
    assert child2["status"] == "pending"


class TestFakeStorageLineage:
    """协议基准（纯内存）。"""

    async def test_matrix(self):
        await _lineage_matrix_asserts(FakeStorage())


@pytest.mark.integration
@requires_taskbase
class TestTaskbaseLineage:
    """taskbase 存储验收（需 Redis）。

    签名纪律：taskbase initialize_task 的第二位置参数是 expiry（非 initial_status），
    跨实现调用必须使用关键字参数。
    """

    async def test_matrix(self, redis_client):
        storage = get_default_storage(redis_client)
        if hasattr(storage, "connect"):
            await storage.connect()

        # taskbase position signature differs: all keyword after first parameter
        await storage.initialize_task(task_id="parent", initial_status="pending")
        assert await storage.initialize_task(
            task_id="child_1", initial_status="pending", parent_task_id="parent"
        ) is True
        assert await storage.initialize_task(
            task_id="child_2", initial_status="pending", parent_task_id="parent"
        ) is True

        children = await storage.list_children("parent")
        assert set(children) == {"child_1", "child_2"}

        # record carries lineage fields
        child = await storage.get_task("child_1")
        assert child["parent_task_id"] == "parent"

        # idempotent
        assert await storage.initialize_task(
            task_id="child_1", initial_status="pending", if_not_exists=True
        ) is False

        # after idempotent skip status not wiped
        # (validate=False: taskflow vocabulary pending→running not direct,
        # this test pins idempotent semantics not vocabulary validation)
        await storage.update_task("child_1", {"status": "running"},
                                  validate_status_transfer=False)
        assert await storage.initialize_task(
            task_id="child_1", initial_status="pending", if_not_exists=True
        ) is False
        child = await storage.get_task("child_1")
        assert child["status"] == "running"