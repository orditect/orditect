"""Local-file dependency-graph domain part (pure-edge facts, T12)."""

from __future__ import annotations

import asyncio
from collections import deque
from pathlib import Path

from orditect.protocol import (
    CapabilitySet,
    DependencyEdge,
    DependencyGraph,
    IdempotencyConflictError,
)

from orditect.adapter.local._common import append_envelope, iter_envelopes


class LocalDependencyPart:
    """Implements DependencyWriter + DependencyReader over deps.ndjson."""

    def __init__(self, root: Path) -> None:
        self._file = root / "deps.ndjson"
        self._lock = asyncio.Lock()

    @property
    def capabilities(self) -> CapabilitySet:
        return CapabilitySet(
            dependency_sink=True,
            dependency_query=True,
            concurrency_domain="process",
        )

    # ---------- writer ----------

    async def write_dependency(self, edge: DependencyEdge) -> None:
        async with self._lock:
            existing = self._find(edge.child_id, edge.parent_id)
            if existing is not None:
                # Same key: identical is_primary -> silent dedup; different -> conflict (T4).
                if existing.is_primary != edge.is_primary:
                    raise IdempotencyConflictError(
                        f"edge {(edge.child_id, edge.parent_id)} rewritten "
                        f"with different is_primary"
                    )
                return
            append_envelope(self._file, "edge_write", edge.to_payload())

    def _find(self, child_id: str, parent_id: str) -> DependencyEdge | None:
        for env in iter_envelopes(self._file):
            data = env["data"]
            if data.get("child_id") == child_id and data.get("parent_id") == parent_id:
                return DependencyEdge.model_validate(data)
        return None

    def _all(self) -> list[DependencyEdge]:
        return [
            DependencyEdge.model_validate(env["data"])
            for env in iter_envelopes(self._file)
        ]

    # ---------- reader ----------

    async def read_graph(self, root_task_id: str) -> DependencyGraph:
        edges = self._all()
        # Bidirectional adjacency (upstream + downstream), BFS with a visited
        # set — cycle-safe by construction (T12).
        adjacency: dict[str, set[str]] = {}
        for e in edges:
            adjacency.setdefault(e.child_id, set()).add(e.parent_id)
            adjacency.setdefault(e.parent_id, set()).add(e.child_id)

        visited = {root_task_id}
        queue: deque[str] = deque([root_task_id])
        while queue:
            node = queue.popleft()
            for nxt in adjacency.get(node, ()):
                if nxt not in visited:
                    visited.add(nxt)
                    queue.append(nxt)

        return DependencyGraph(
            root_task_id=root_task_id,
            task_ids=sorted(visited),
            edges=[e for e in edges if e.child_id in visited],
        )

    async def all_edges(self) -> list[DependencyEdge]:
        return self._all()

    async def children_of(self, parent_task_id: str) -> list[str]:
        return sorted(e.child_id for e in self._all() if e.parent_id == parent_task_id)

    async def parents_of(self, child_task_id: str) -> list[str]:
        return sorted(e.parent_id for e in self._all() if e.child_id == child_task_id)