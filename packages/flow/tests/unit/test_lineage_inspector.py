"""专题 4 钉扎：观测谱系化（嵌套任务观测数据按谱系串成树）。

递归愿景验收 #4：一次嵌套任务的全部观测数据（状态/资源/结构）
能按 parent_task_id 组装成一棵树 + 谱系路径 + 树统计。
"""
import asyncio
import time
from typing import Any, Dict, List, Optional

import pytest

from orditect.flow import (
    BaseBackEndTask,
    LineageInspector,
    TaskOrchestrator,
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
        return [tid for tid, t in self._tasks.items() if t.get("status") == status]

    async def bulk_get_tasks(self, task_ids):
        return [dict(self._tasks.get(tid, {})) for tid in task_ids]


async def _wait_for(predicate, timeout: float = 3.0, interval: float = 0.02):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        await asyncio.sleep(interval)
    return False


class TestBuildTree:
    """谱系树组装（手工谱系 + 真实嵌套两个层次）。"""

    async def test_manual_lineage_tree(self):
        """手工搭建的三层谱系 → 树形结构正确。"""
        storage = FakeStorage()
        await storage.initialize_task("root", "succeeded")
        await storage.update_task("root", {"resource": "task_agent"})
        await storage.initialize_task("child-1", "succeeded", parent_task_id="root")
        await storage.update_task("child-1", {"resource": "llm_pool"})
        await storage.initialize_task("child-2", "failed", parent_task_id="root")
        await storage.initialize_task("grandchild", "succeeded", parent_task_id="child-1")

        inspector = LineageInspector(storage)
        tree = await inspector.build_tree("root")

        assert tree["task_id"] == "root"
        assert tree["status"] == "succeeded"
        assert tree["resource"] == "task_agent"
        assert len(tree["children"]) == 2

        child1 = next(c for c in tree["children"] if c["task_id"] == "child-1")
        assert child1["resource"] == "llm_pool"
        assert len(child1["children"]) == 1
        assert child1["children"][0]["task_id"] == "grandchild"

        child2 = next(c for c in tree["children"] if c["task_id"] == "child-2")
        assert child2["status"] == "failed"
        assert child2["children"] == []

    async def test_tree_with_extra_fields(self):
        """include_fields 附加字段（如 result）透传到节点。"""
        storage = FakeStorage()
        await storage.initialize_task("root", "succeeded")
        await storage.update_task("root", {"result": {"report_s3_key": "s3://bucket/r1"}})
        await storage.initialize_task("child", "succeeded", parent_task_id="root")

        inspector = LineageInspector(storage)
        tree = await inspector.build_tree("root", include_fields=["result"])

        assert tree["result"] == {"report_s3_key": "s3://bucket/r1"}
        assert "result" not in tree["children"][0]  # 子节点无该字段则不携带

    async def test_tree_cycle_protection(self):
        """谱系成环时深度/环防护（不死循环）。"""
        storage = FakeStorage()
        await storage.initialize_task("a", "running")
        await storage.initialize_task("b", "running", parent_task_id="a")
        await storage.update_task("a", {"parent_task_id": "b"})  # 成环

        inspector = LineageInspector(storage)
        # not hang means pass
        tree = await asyncio.wait_for(inspector.build_tree("a"), timeout=3.0)
        assert tree["task_id"] == "a"


class TestLineagePath:
    """谱系路径（root → 当前节点）。"""

    async def test_path_three_levels(self):
        storage = FakeStorage()
        await storage.initialize_task("root", "running")
        await storage.initialize_task("mid", "running", parent_task_id="root")
        await storage.initialize_task("leaf", "running", parent_task_id="mid")

        inspector = LineageInspector(storage)
        path = await inspector.get_lineage_path("leaf")
        assert path == ["root", "mid", "leaf"]

    async def test_path_root_is_single(self):
        storage = FakeStorage()
        await storage.initialize_task("root", "running")

        inspector = LineageInspector(storage)
        assert await inspector.get_lineage_path("root") == ["root"]


class TestTreeStats:
    """树统计聚合视图。"""

    async def test_stats_aggregation(self):
        storage = FakeStorage()
        await storage.initialize_task("root", "succeeded")
        await storage.update_task("root", {"resource": "task_agent"})
        await storage.initialize_task("c1", "succeeded", parent_task_id="root")
        await storage.update_task("c1", {"resource": "llm_pool"})
        await storage.initialize_task("c2", "succeeded", parent_task_id="root")
        await storage.update_task("c2", {"resource": "llm_pool"})
        await storage.initialize_task("c3", "failed", parent_task_id="root")

        inspector = LineageInspector(storage)
        stats = await inspector.get_tree_stats("root")

        assert stats["total"] == 4
        assert stats["by_status"] == {"succeeded": 3, "failed": 1}
        assert stats["by_resource"] == {"task_agent": 1, "llm_pool": 2}


class TestNestedSubmitTree:
    """端到端：真实嵌套 submit 的任务树（验收 #4 的完整形态）。"""

    async def test_real_nested_task_tree(self):
        """嵌套 submit 后，整棵树的观测数据可组装（含资源账）。"""
        storage = FakeStorage()
        orchestrator = TaskOrchestrator(storage, governor=None)

        class LeafTask(BaseBackEndTask):
            async def execute(self, task_id: str, **kwargs):
                return {"leaf": True}

        class MidTask(BaseBackEndTask):
            async def execute(self, task_id: str, **kwargs):
                cid = await orchestrator.submit(LeafTask(storage))
                await orchestrator.wait_terminal(cid, timeout=3.0)
                return {"mid": True}

        class RootTask(BaseBackEndTask):
            async def execute(self, task_id: str, **kwargs):
                cid = await orchestrator.submit(MidTask(storage))
                await orchestrator.wait_terminal(cid, timeout=3.0)
                return {"root": True}

        root_id = await orchestrator.submit(RootTask(storage))
        record = await orchestrator.wait_terminal(root_id, timeout=5.0)
        assert record["status"] == "succeeded"

        # lineage tree assembly
        inspector = LineageInspector(storage)
        tree = await inspector.build_tree(root_id)

        assert tree["task_id"] == root_id
        assert tree["status"] == "succeeded"
        assert len(tree["children"]) == 1
        mid = tree["children"][0]
        assert mid["status"] == "succeeded"
        assert len(mid["children"]) == 1
        leaf = mid["children"][0]
        assert leaf["status"] == "succeeded"
        assert leaf["children"] == []

        # tree statistics
        stats = await inspector.get_tree_stats(root_id)
        assert stats["total"] == 3
        assert stats["by_status"] == {"succeeded": 3}