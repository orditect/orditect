"""In-memory snapshot domain part.

Implements the full snapshot contract surface, including the T3/T4/T11
write semantics and cycle-safe, depth-bounded tree traversal (contract
terms, not implementation details).
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any

from orditect.protocol import (
    CapabilitySet,
    InvalidQueryError,
    Page,
    Sort,
    SortDirection,
    TaskSnapshot,
    TerminalStateViolationError,
    TimeRange,
)
from orditect.protocol.mechanism import (
    GROUP_BY_FIELDS,
    SORT_FIELDS,
    fold_snapshot_rows,
    idempotent_payload_equal,
)

_MAX_TRAVERSAL_DEPTH = 32  # contract-termed traversal bound


class MemorySnapshotPart:
    """Implements SnapshotWriter + SnapshotReader."""

    def __init__(self) -> None:
        self._snaps: dict[tuple[str, str, str], TaskSnapshot] = {}
        self._terminal: set[tuple[str, str, str]] = set()
        self._lock = asyncio.Lock()

    @property
    def capabilities(self) -> CapabilitySet:
        return CapabilitySet(snapshot_sink=True, snapshot_query=True)

    # ---------- writer ----------

    async def save(self, snapshot: TaskSnapshot) -> None:
        async with self._lock:
            key = (snapshot.task_id, snapshot.step, snapshot.execution_id)
            existing = self._snaps.get(key)
            if key in self._terminal and existing is not None:
                # T3: only a non-empty, differing status is a state
                # mutation. An empty status is the absence of intent
                # (adjudicated v0.1.5) — it never triggers the guard.
                if snapshot.status and snapshot.status != existing.status:
                    raise TerminalStateViolationError(
                        f"state mutation within terminal generation: {key}"
                    )
            if existing is not None:
                record = fold_snapshot_rows(
                    [existing.model_dump(), snapshot.model_dump()]
                )
                self._snaps[key] = TaskSnapshot.model_validate(record)
                return
            self._snaps[key] = snapshot

    async def save_terminal(self, snapshot: TaskSnapshot) -> None:
        async with self._lock:
            key = (snapshot.task_id, snapshot.step, snapshot.execution_id)
            existing = self._snaps.get(key)
            if key in self._terminal and existing is not None:
                # T4: re-saving a terminal generation with identical business
                # content (mechanism clock fields excluded) is a silent dedup;
                # differing content is a conflict (T3).
                if not idempotent_payload_equal(
                    existing.model_dump(), snapshot.model_dump()
                ):
                    raise TerminalStateViolationError(
                        f"conflicting re-save of terminal generation: {key}"
                    )
                return
            self._snaps[key] = snapshot
            self._terminal.add(key)

    # ---------- reader ----------

    def _alive(self, s: TaskSnapshot) -> bool:
        return s.expire_at is None or s.expire_at > datetime.now(UTC)

    async def get(
        self,
        task_id: str,
        step: str,
        *,
        execution_id: str | None = None,
    ) -> TaskSnapshot | None:
        rows = [
            s for (t, st, e), s in self._snaps.items()
            if t == task_id and st == step
            and (execution_id is None or e == execution_id)
            and self._alive(s)
        ]
        if not rows:
            return None
        rows.sort(key=lambda s: s.created_at)
        return rows[-1]

    async def list_versions(
        self,
        task_id: str,
        step: str,
        *,
        page: Page | None = None,
        sort: Sort | None = None,
    ) -> list[TaskSnapshot]:
        rows = [
            s for (t, st, _), s in self._snaps.items()
            if t == task_id and st == step and self._alive(s)
        ]
        from datetime import UTC, datetime
        sort = sort or Sort()
        if sort.field not in SORT_FIELDS["snapshot"]:
            raise InvalidQueryError(
                f"sort.field {sort.field!r} outside the snapshot mechanism "
                f"whitelist: {sorted(SORT_FIELDS['snapshot'])}"
            )

        def _key(s: TaskSnapshot):
            value = getattr(s, sort.field)
            if sort.field == "expire_at":
                return (value is None, value or datetime.max.replace(tzinfo=UTC))
            return value

        rows.sort(key=_key, reverse=sort.direction is SortDirection.DESC)
        page = page or Page()
        return rows[page.offset: page.offset + page.limit]

    def _latest_per_node(self, snaps: list[TaskSnapshot]) -> list[TaskSnapshot]:
        latest: dict[tuple[str, str], TaskSnapshot] = {}
        for s in snaps:
            key = (s.task_id, s.step)
            if key not in latest or s.created_at >= latest[key].created_at:
                latest[key] = s
        return list(latest.values())

    async def get_tree(
        self,
        root_task_id: str,
        *,
        max_depth: int | None = None,
        latest_only: bool = True,
    ) -> list[TaskSnapshot]:
        """Cycle-safe, depth-bounded subtree collection (contract terms)."""
        alive = [s for s in self._snaps.values() if self._alive(s)]
        by_parent: dict[str, list[TaskSnapshot]] = {}
        for s in alive:
            if s.parent_task_id is not None:
                by_parent.setdefault(s.parent_task_id, []).append(s)

        depth_limit = max_depth if max_depth is not None else _MAX_TRAVERSAL_DEPTH
        out: list[TaskSnapshot] = []
        visited: set[str] = set()

        def walk(task_id: str, depth: int) -> None:
            if depth > depth_limit or task_id in visited:
                return  # cycle-safe + depth-bounded (contract terms)
            visited.add(task_id)
            nodes = [s for s in alive if s.task_id == task_id]
            out.extend(nodes)
            for child in by_parent.get(task_id, []):
                walk(child.task_id, depth + 1)

        walk(root_task_id, 0)
        return self._latest_per_node(out) if latest_only else out

    async def list_children(
        self,
        parent_task_id: str,
        *,
        latest_only: bool = True,
    ) -> list[TaskSnapshot]:
        rows = [
            s for s in self._snaps.values()
            if s.parent_task_id == parent_task_id and self._alive(s)
        ]
        return self._latest_per_node(rows) if latest_only else rows

    async def get_ancestors(
        self,
        task_id: str,
        *,
        include_self: bool = False,
    ) -> list[TaskSnapshot]:
        by_id: dict[str, TaskSnapshot] = {}
        for s in self._snaps.values():
            if self._alive(s):
                cur = by_id.get(s.task_id)
                if cur is None or s.created_at >= cur.created_at:
                    by_id[s.task_id] = s

        chain: list[TaskSnapshot] = []
        visited: set[str] = set()
        current = by_id.get(task_id)
        if current is None:
            return []
        if include_self:
            chain.append(current)
            visited.add(task_id)

        depth = 0
        while current is not None and current.parent_task_id is not None:
            if depth >= _MAX_TRAVERSAL_DEPTH:
                break
            parent_id = current.parent_task_id
            if parent_id in visited:
                break  # cycle-safe (contract term)
            visited.add(parent_id)
            parent = by_id.get(parent_id)
            if parent is None:
                break
            chain.append(parent)
            current = parent
            depth += 1

        chain.reverse()  # root first
        return chain

    async def query(
        self,
        *,
        status: str | None = None,
        parent_task_id: str | None = None,
        time_range: TimeRange | None = None,
        page: Page | None = None,
        sort: Sort | None = None,
    ) -> list[TaskSnapshot]:
        rows = self._latest_per_node([s for s in self._snaps.values() if self._alive(s)])
        if status is not None:
            rows = [s for s in rows if s.status == status]
        if parent_task_id is not None:
            rows = [s for s in rows if s.parent_task_id == parent_task_id]
        if time_range is not None:
            if time_range.start is not None:
                rows = [s for s in rows if s.created_at >= time_range.start]
            if time_range.end is not None:
                rows = [s for s in rows if s.created_at < time_range.end]
        from datetime import UTC, datetime
        sort = sort or Sort()
        if sort.field not in SORT_FIELDS["snapshot"]:
            raise InvalidQueryError(
                f"sort.field {sort.field!r} outside the snapshot mechanism "
                f"whitelist: {sorted(SORT_FIELDS['snapshot'])}"
            )

        def _key(s: TaskSnapshot):
            value = getattr(s, sort.field)
            if sort.field == "expire_at":
                return (value is None, value or datetime.max.replace(tzinfo=UTC))
            return value

        rows.sort(key=_key, reverse=sort.direction is SortDirection.DESC)
        page = page or Page()
        return rows[page.offset: page.offset + page.limit]

    async def aggregate(
        self,
        *,
        group_by: str,
        parent_task_id: str | None = None,
        time_range: TimeRange | None = None,
    ) -> dict[str, Any]:
        if group_by not in GROUP_BY_FIELDS["snapshot"]:
            raise InvalidQueryError(
                f"group_by {group_by!r} outside the snapshot mechanism "
                f"whitelist: {sorted(GROUP_BY_FIELDS['snapshot'])}"
            )
        rows = self._latest_per_node([s for s in self._snaps.values() if self._alive(s)])
        if parent_task_id is not None:
            rows = [s for s in rows if s.parent_task_id == parent_task_id]
        if time_range is not None:
            if time_range.start is not None:
                rows = [s for s in rows if s.created_at >= time_range.start]
            if time_range.end is not None:
                rows = [s for s in rows if s.created_at < time_range.end]

        out: dict[str, Any] = {}
        for s in rows:
            gval = str(getattr(s, group_by, "unknown"))
            bucket = out.setdefault(gval, {"count": 0, "cost": {}})
            bucket["count"] += 1
            for k, v in (s.cost or {}).items():
                bucket["cost"][k] = bucket["cost"].get(k, 0.0) + v
        return out