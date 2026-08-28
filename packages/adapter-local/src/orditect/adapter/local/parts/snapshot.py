"""Local-file snapshot domain part.

Snapshots persist as append-only op-envelope rows (op "save" /
"save_terminal"). Read paths fold the stream: T3 terminal rules, T4
idempotent re-save, T1 lazy expiry, and cycle-safe depth-bounded tree
traversal are evaluated against the folded view.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from pathlib import Path
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
    idempotent_payload_equal,
)

from orditect.adapter.local._common import append_envelope, iter_envelopes, parse_dt

_MAX_TRAVERSAL_DEPTH = 32  # contract-termed traversal bound


class LocalSnapshotPart:
    """Implements SnapshotWriter + SnapshotReader over snapshots.ndjson."""

    def __init__(self, root: Path) -> None:
        self._file = root / "snapshots.ndjson"
        self._lock = asyncio.Lock()

    @property
    def capabilities(self) -> CapabilitySet:
        return CapabilitySet(
            snapshot_sink=True, snapshot_query=True, concurrency_domain="process"
        )

    # ---------- folding ----------

    def _fold(self) -> tuple[dict, set]:
        """Fold the stream into (latest row per key, terminal keys).

        T3 second face: non-state fields may merge into a terminal
        generation's record to complete it; status comes from the LATEST
        row in the stream (it is state, always advancing with the newest
        write), while non-state fields merge (incoming overwrites, absent
        fields preserved).
        """
        rows: dict[tuple[str, str, str], dict] = {}
        terminal: set[tuple[str, str, str]] = set()
        for env in iter_envelopes(self._file):
            data = env["data"]
            key = (
                data.get("task_id", ""),
                data.get("step", ""),
                data.get("execution_id", ""),
            )
            existing = rows.get(key)
            if existing is None:
                rows[key] = data
            else:
                # Status: always the latest row's status (state advances
                # with the newest write).
                # Non-state fields: incoming overwrites; previously recorded
                # fields are preserved only when absent from the incoming row.
                merged = dict(data)
                for f in ("parent_task_id", "cost", "output_pointer", "error",
                          "input_pointer", "model", "expire_at"):
                    if f not in data and f in existing:
                        merged[f] = existing[f]
                rows[key] = merged
            if env.get("op") == "save_terminal":
                terminal.add(key)
        return rows, terminal

    # ---------- writer ----------

    async def save(self, snapshot: TaskSnapshot) -> None:
        async with self._lock:
            rows, terminal = self._fold()
            key = (snapshot.task_id, snapshot.step, snapshot.execution_id)
            existing = rows.get(key)
            if key in terminal and existing is not None:
                if snapshot.status != existing.get("status", ""):
                    # T3: state mutation within a terminal generation.
                    raise TerminalStateViolationError(
                        f"state mutation within terminal generation: {key}"
                    )
            append_envelope(self._file, "save", snapshot.to_payload())

    async def save_terminal(self, snapshot: TaskSnapshot) -> None:
        async with self._lock:
            rows, terminal = self._fold()
            key = (snapshot.task_id, snapshot.step, snapshot.execution_id)
            existing = rows.get(key)
            if key in terminal and existing is not None:
                # T4: identical business content (mechanism clock fields
                # excluded) is a silent dedup; differing content is a
                # conflict (T3).
                if not idempotent_payload_equal(
                    existing, snapshot.to_payload()
                ):
                    raise TerminalStateViolationError(
                        f"conflicting re-save of terminal generation: {key}"
                    )
                return
            append_envelope(self._file, "save_terminal", snapshot.to_payload())

    # ---------- reader ----------

    @staticmethod
    def _alive(data: dict) -> bool:
        expire = data.get("expire_at")
        if expire is None:
            return True
        dt = parse_dt(expire)
        return dt is None or dt > datetime.now(UTC)

    def _validate_sort(self, sort: Sort | None) -> Sort:
        sort = sort or Sort()
        if sort.field not in SORT_FIELDS["snapshot"]:
            raise InvalidQueryError(
                f"sort.field {sort.field!r} outside the snapshot mechanism "
                f"whitelist: {sorted(SORT_FIELDS['snapshot'])}"
            )
        return sort

    @staticmethod
    def _sort_key(data: dict, field: str):
        return str(data.get(field, ""))

    async def get(
        self,
        task_id: str,
        step: str,
        *,
        execution_id: str | None = None,
    ) -> TaskSnapshot | None:
        rows, _ = self._fold()
        candidates = [
            d
            for (t, st, e), d in rows.items()
            if t == task_id and st == step
            and (execution_id is None or e == execution_id)
            and self._alive(d)
        ]
        if not candidates:
            return None
        candidates.sort(key=lambda d: str(d.get("created_at", "")))
        return TaskSnapshot.model_validate(candidates[-1])

    async def list_versions(
        self,
        task_id: str,
        step: str,
        *,
        page: Page | None = None,
        sort: Sort | None = None,
    ) -> list[TaskSnapshot]:
        sort = self._validate_sort(sort)
        rows, _ = self._fold()
        candidates = [
            d
            for (t, st, _), d in rows.items()
            if t == task_id and st == step and self._alive(d)
        ]
        candidates.sort(
            key=lambda d: self._sort_key(d, sort.field),
            reverse=sort.direction is SortDirection.DESC,
        )
        page = page or Page()
        return [
            TaskSnapshot.model_validate(d)
            for d in candidates[page.offset: page.offset + page.limit]
        ]

    @staticmethod
    def _latest_per_node(datas: list[dict]) -> list[dict]:
        latest: dict[tuple[str, str], dict] = {}
        for d in datas:
            key = (d.get("task_id", ""), d.get("step", ""))
            cur = latest.get(key)
            if cur is None or str(d.get("created_at", "")) >= str(
                cur.get("created_at", "")
            ):
                latest[key] = d
        return list(latest.values())

    async def get_tree(
        self,
        root_task_id: str,
        *,
        max_depth: int | None = None,
        latest_only: bool = True,
    ) -> list[TaskSnapshot]:
        """Cycle-safe, depth-bounded subtree collection (contract terms)."""
        rows, _ = self._fold()
        alive = [d for d in rows.values() if self._alive(d)]
        by_parent: dict[str, list[dict]] = {}
        for d in alive:
            parent = d.get("parent_task_id")
            if parent is not None:
                by_parent.setdefault(parent, []).append(d)

        depth_limit = max_depth if max_depth is not None else _MAX_TRAVERSAL_DEPTH
        out: list[dict] = []
        visited: set[str] = set()

        def walk(task_id: str, depth: int) -> None:
            if depth > depth_limit or task_id in visited:
                return  # cycle-safe + depth-bounded (contract terms)
            visited.add(task_id)
            out.extend(d for d in alive if d.get("task_id") == task_id)
            for child in by_parent.get(task_id, []):
                walk(child["task_id"], depth + 1)

        walk(root_task_id, 0)
        folded = self._latest_per_node(out) if latest_only else out
        return [TaskSnapshot.model_validate(d) for d in folded]

    async def list_children(
        self,
        parent_task_id: str,
        *,
        latest_only: bool = True,
    ) -> list[TaskSnapshot]:
        rows, _ = self._fold()
        children = [
            d
            for d in rows.values()
            if d.get("parent_task_id") == parent_task_id and self._alive(d)
        ]
        folded = self._latest_per_node(children) if latest_only else children
        return [TaskSnapshot.model_validate(d) for d in folded]

    async def get_ancestors(
        self,
        task_id: str,
        *,
        include_self: bool = False,
    ) -> list[TaskSnapshot]:
        rows, _ = self._fold()
        by_id: dict[str, dict] = {}
        for d in rows.values():
            if not self._alive(d):
                continue
            cur = by_id.get(d.get("task_id", ""))
            if cur is None or str(d.get("created_at", "")) >= str(
                cur.get("created_at", "")
            ):
                by_id[d.get("task_id", "")] = d

        chain: list[dict] = []
        visited: set[str] = set()
        current = by_id.get(task_id)
        if current is None:
            return []
        if include_self:
            chain.append(current)
            visited.add(task_id)

        depth = 0
        while current is not None and current.get("parent_task_id") is not None:
            if depth >= _MAX_TRAVERSAL_DEPTH:
                break
            parent_id = current["parent_task_id"]
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
        return [TaskSnapshot.model_validate(d) for d in chain]

    async def query(
        self,
        *,
        status: str | None = None,
        parent_task_id: str | None = None,
        time_range: TimeRange | None = None,
        page: Page | None = None,
        sort: Sort | None = None,
    ) -> list[TaskSnapshot]:
        sort = self._validate_sort(sort)
        rows, _ = self._fold()
        candidates = self._latest_per_node(
            [d for d in rows.values() if self._alive(d)]
        )
        if status is not None:
            candidates = [d for d in candidates if d.get("status") == status]
        if parent_task_id is not None:
            candidates = [
                d for d in candidates if d.get("parent_task_id") == parent_task_id
            ]
        if time_range is not None:
            if time_range.start is not None:
                candidates = [
                    d
                    for d in candidates
                    if (dt := parse_dt(d.get("created_at"))) is not None
                    and dt >= time_range.start
                ]
            if time_range.end is not None:
                candidates = [
                    d
                    for d in candidates
                    if (dt := parse_dt(d.get("created_at"))) is not None
                    and dt < time_range.end
                ]
        candidates.sort(
            key=lambda d: self._sort_key(d, sort.field),
            reverse=sort.direction is SortDirection.DESC,
        )
        page = page or Page()
        return [
            TaskSnapshot.model_validate(d)
            for d in candidates[page.offset: page.offset + page.limit]
        ]

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
        rows, _ = self._fold()
        candidates = self._latest_per_node(
            [d for d in rows.values() if self._alive(d)]
        )
        if parent_task_id is not None:
            candidates = [
                d for d in candidates if d.get("parent_task_id") == parent_task_id
            ]
        if time_range is not None:
            if time_range.start is not None:
                candidates = [
                    d
                    for d in candidates
                    if (dt := parse_dt(d.get("created_at"))) is not None
                    and dt >= time_range.start
                ]
            if time_range.end is not None:
                candidates = [
                    d
                    for d in candidates
                    if (dt := parse_dt(d.get("created_at"))) is not None
                    and dt < time_range.end
                ]

        out: dict[str, Any] = {}
        for d in candidates:
            gval = str(d.get(group_by, "unknown"))
            bucket = out.setdefault(gval, {"count": 0, "cost": {}})
            bucket["count"] += 1
            for k, v in (d.get("cost") or {}).items():
                bucket["cost"][k] = bucket["cost"].get(k, 0.0) + v
        return out