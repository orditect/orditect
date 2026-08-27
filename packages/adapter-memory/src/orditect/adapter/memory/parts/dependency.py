"""In-memory dependency-graph domain part (pure-edge facts, T12)."""

from __future__ import annotations

import asyncio
from collections import deque

from orditect.protocol import (
    CapabilitySet,
    DependencyEdge,
    DependencyGraph,
    IdempotencyConflictError,
)


class MemoryDependencyPart:
    """Implements DependencyWriter + DependencyReader (append-only edge facts)."""

    def __init__(self) -> None:
        self._edges: dict[tuple[str, str], DependencyEdge] = {}
        self._lock = asyncio.Lock()

    @property
    def capabilities(self) -> CapabilitySet:
        return CapabilitySet(dependency_sink=True, dependency_query=True)

    # ---------- writer ----------

    async def write_dependency(self, edge: DependencyEdge) -> None:
        async with self._lock:
            key = (edge.child_id, edge.parent_id)
            existing = self._edges.get(key)
            if existing is None:
                self._edges[key] = edge
                return
            # Same key: identical is_primary -> silent dedup; different -> conflict (T4).
            if existing.is_primary != edge.is_primary:
                raise IdempotencyConflictError(
                    f"edge {key} rewritten with different is_primary"
                )

    # ---------- reader ----------

    async def read_graph(self, root_task_id: str) -> DependencyGraph:
        # Bidirectional adjacency (upstream + downstream), BFS with a visited
        # set — cycle-safe by construction (T12).
        adjacency: dict[str, set[str]] = {}
        for child_id, parent_id in self._edges:
            adjacency.setdefault(child_id, set()).add(parent_id)
            adjacency.setdefault(parent_id, set()).add(child_id)

        visited = {root_task_id}
        queue: deque[str] = deque([root_task_id])
        while queue:
            node = queue.popleft()
            for nxt in adjacency.get(node, ()):
                if nxt not in visited:
                    visited.add(nxt)
                    queue.append(nxt)

        edges = [e for e in self._edges.values() if e.child_id in visited]
        return DependencyGraph(
            root_task_id=root_task_id,
            task_ids=sorted(visited),
            edges=edges,
        )

    async def all_edges(self) -> list[DependencyEdge]:
        return list(self._edges.values())

    async def children_of(self, parent_task_id: str) -> list[str]:
        return sorted(c for (c, p) in self._edges if p == parent_task_id)

    async def parents_of(self, child_task_id: str) -> list[str]:
        return sorted(p for (c, p) in self._edges if c == child_task_id)