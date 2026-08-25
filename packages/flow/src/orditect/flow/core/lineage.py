"""Observability lineage (Topic 4): assemble observation data of nested tasks into a tree by lineage.

Data foundation (all already exist, zero new writes):
- parent_task_id lineage index (batch 2/3)
- resource resource ledger (batch 4 executor registration)

This module only performs assembly from "flat data → tree structure" (recursive traversal);
rendering/visualization/audit consumption are left to business side.

Usage example:
    inspector = LineageInspector(storage)

    # Whole task tree (recursively from root)
    tree = await inspector.build_tree(root_task_id)
    # {
    #   "task_id": "root", "status": "succeeded", "resource": "task_agent",
    #   "children": [
    #     {"task_id": "child-1", "status": "succeeded", "resource": "llm_pool",
    #      "children": [...]},
    #     ...
    #   ]
    # }

    # Lineage path (root → ... → current node)
    path = await inspector.get_lineage_path(task_id)
    path = await inspector.get_lineage_path(task_id)
    # ["root", "parent", "current"]
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from orditect.flow.protocols.storage import TaskStorageProtocol

logger = logging.getLogger(__name__)

# : maximum depth of lineage traversal (consistent with orchestrator cascade limit, prevents cycles)
_MAX_LINEAGE_DEPTH = 32


class LineageInspector:
    """Recursively assemble the whole task tree starting from the root task.

    Args:
        root_task_id: Root task ID
        include_fields: Additional task record fields to include per node
            (default includes status/resource/cancel_requested;
            e.g. ["result", "error"] to attach)

    Returns:
        Nested dict tree:
        {
            "task_id": ...,
            "status": ...,
            "resource": ... (None if absent),
            "cancel_requested": ...,
            "children": [recursive subtrees],
            ...additional fields from include_fields
        }

    Raises:
        TaskNotFoundError: Root task does not exist
    """

    def __init__(self, storage: TaskStorageProtocol):
        self._storage = storage

    async def build_tree(
        self,
        root_task_id: str,
        *,
        include_fields: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Recursively assemble the whole task tree starting from the root task.

        Args:
            root_task_id: Root task ID
            include_fields: Additional task record fields to include per node
                (default includes status/resource/cancel_requested;
                e.g. ["result", "error"] to attach)

        Returns:
            Nested dict tree:
            {
                "task_id": ...,
                "status": ...,
                "resource": ... (None if absent),
                "cancel_requested": ...,
                "children": [recursive subtrees],
                ...additional fields from include_fields
            }

        Raises:
            TaskNotFoundError: Root task does not exist
        """
        extra = include_fields or []
        return await self._build_node(root_task_id, extra, depth=0, visited=set())

    async def _build_node(
        self,
        task_id: str,
        extra_fields: List[str],
        *,
        depth: int,
        visited: set,
    ) -> Dict[str, Any]:
        """Recursively assemble a single node (with depth limit and cycle protection)."""
        if depth >= _MAX_LINEAGE_DEPTH:
            logger.warning(f"Lineage depth limit reached, possible cycle: {task_id}")
            return {"task_id": task_id, "error": "depth_limit_reached", "children": []}
        if task_id in visited:
            logger.warning(f"Lineage cycle detected: {task_id}")
            return {"task_id": task_id, "error": "cycle_detected", "children": []}
        visited.add(task_id)

        record = await self._storage.get_task(task_id)  # TaskNotFoundError 原样传播

        node: Dict[str, Any] = {
            "task_id": task_id,
            "status": record.get("status"),
            "resource": record.get("resource"),  # 批次 4 资源账（无则 None）
            "cancel_requested": record.get("cancel_requested", False),
        }
        # additional fields (result/error/payload pointers etc., business as needed)
        for field in extra_fields:
            if field in record:
                node[field] = record[field]

        # recursive child nodes
        children_ids = await self._storage.list_children(task_id)
        node["children"] = [
            await self._build_node(cid, extra_fields, depth=depth + 1, visited=visited)
            for cid in children_ids
        ]
        return node

    async def get_lineage_path(self, task_id: str) -> List[str]:
        """Get the lineage path (root → ... → current node's task_id chain).

        Used to annotate observation data with "which task chain this record belongs to".

        Returns:
            List of task_ids from root to current node (inclusive);
            if no lineage (root task), returns [task_id]
        """
        path = [task_id]
        visited = {task_id}
        current = task_id

        for _ in range(_MAX_LINEAGE_DEPTH):
            try:
                record = await self._storage.get_task(current)
            except Exception:
                break  # 查询失败：返回已收集的路径
            parent = record.get("parent_task_id")
            if parent is None:
                break  # 到根
            if parent in visited:
                logger.warning(f"Lineage cycle detected at: {parent}")
                break
            path.insert(0, parent)
            visited.add(parent)
            current = parent

        return path

    async def get_tree_stats(self, root_task_id: str) -> Dict[str, Any]:
        """Task tree statistics (node count / status distribution / resource distribution) — aggregated view for observation dashboards.

        Returns:
            {
                "total": total nodes,
                "by_status": {"succeeded": 3, "failed": 1, ...},
                "by_resource": {"llm_pool": 2, "task_agent": 1, ...},
            }
        """
        tree = await self.build_tree(root_task_id)
        stats: Dict[str, Any] = {"total": 0, "by_status": {}, "by_resource": {}}

        def _walk(node: Dict[str, Any]) -> None:
            stats["total"] += 1
            status = node.get("status") or "unknown"
            stats["by_status"][status] = stats["by_status"].get(status, 0) + 1
            resource = node.get("resource")
            if resource:
                stats["by_resource"][resource] = stats["by_resource"].get(resource, 0) + 1
            for child in node.get("children", []):
                _walk(child)

        _walk(tree)
        return stats