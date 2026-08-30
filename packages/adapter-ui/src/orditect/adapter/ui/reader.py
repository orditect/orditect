"""TraceBundleReader: consumer-side read over a trace bundle directory.

Parses the trace-bundle data form (ndjson envelope rows + JSON payloads)
produced by orditect-adapter-local (or any conformant adapter) into the
protocol's domain models, without importing orditect-core / orditect-flow.

Boundary discipline:
- Depends only on orditect-protocol (models + CapabilitySet) and stdlib.
- Reads files; never writes. consumer tier: sink cases skip, never fail.
- seed() hook: loads a fixture dict into the in-memory view (for the
  conformance consumer profile's seeded read cases).
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from orditect.protocol import (
    AuditEvent,
    CapabilitySet,
    DependencyEdge,
    DependencyGraph,
    InvalidQueryError,
    Page,
    Sort,
    SortDirection,
    TaskSnapshot,
    TimeRange,
)
from orditect.protocol.mechanism import (
    GROUP_BY_FIELDS,
    SORT_FIELDS,
    fold_snapshot_rows,
)


def _parse_dt(value: Any) -> datetime | None:
    """Parse an ISO datetime string (T7: explicit offset expected)."""
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None

class TraceBundleReader:
    """Read a trace bundle directory into queryable domain views.

    Args:
        root: trace bundle directory (as produced by LocalFileStore).
    """

    def __init__(self, root: str | Path) -> None:
        self._root = Path(root)
        self._snapshots: list[dict] = []
        self._audits: list[dict] = []
        self._edges: list[dict] = []
        self._results: dict[str, dict] = {}
        self._load()

    # ---------- loading ----------

    def _load(self) -> None:
        self._snapshots = self._read_ndjson("snapshots.ndjson")
        self._audits = self._read_ndjson("audit.ndjson")
        self._edges = self._read_ndjson("deps.ndjson")
        results_dir = self._root / "results"
        if results_dir.is_dir():
            for path in sorted(results_dir.glob("*.json")):
                doc = json.loads(path.read_text(encoding="utf-8"))
                self._results[path.stem] = doc

    def _read_ndjson(self, name: str) -> list[dict]:
        path = self._root / name
        if not path.is_file():
            return []
        rows: list[dict] = []
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(row, dict) and isinstance(row.get("data"), dict):
                rows.append(row["data"])
        return rows

    # ---------- seed hook (consumer profile) ----------

    async def seed(self, fixtures: dict) -> None:
        """Load fixture payloads into the in-memory view (idempotent).

        fixtures = {"snapshots": [payload dicts], "edges": [payload dicts]}.
        Datetime fields arrive as ISO strings (converted via fromisoformat).
        """
        for raw in fixtures.get("snapshots", []):
            data = dict(raw)
            for key in ("expire_at", "created_at", "updated_at"):
                if isinstance(data.get(key), str):
                    data[key] = datetime.fromisoformat(data[key])
            self._snapshots.append(data)
        for raw in fixtures.get("edges", []):
            data = dict(raw)
            if isinstance(data.get("registered_at"), str):
                data["registered_at"] = datetime.fromisoformat(
                    data["registered_at"]
                )
            self._edges.append(data)

    # ---------- snapshot view ----------

    @property
    def snapshot(self) -> "SnapshotView":
        return SnapshotView(self._snapshots)

    # ---------- dependency view ----------

    @property
    def dependency(self) -> "DependencyView":
        return DependencyView(self._edges)

    # ---------- audit view ----------

    @property
    def audit(self) -> "AuditView":
        return AuditView(self._audits)

    # ---------- result view ----------

    @property
    def result(self) -> "ResultView":
        return ResultView(self._results)


class SnapshotView:
    """Query surface over snapshot rows (consumer profile)."""

    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows

    @property
    def capabilities(self) -> CapabilitySet:
        return CapabilitySet(snapshot_query=True)

    async def seed(self, fixtures: dict) -> None:
        """Load fixture payloads into the snapshot view (idempotent).

        Conformance consumer hook: fixtures["snapshots"] is a list of
        payload dicts; datetime fields arrive as ISO strings.
        """
        for raw in fixtures.get("snapshots", []):
            data = dict(raw)
            for key in ("expire_at", "created_at", "updated_at"):
                if isinstance(data.get(key), str):
                    data[key] = datetime.fromisoformat(data[key])
            self._rows.append(data)

    def _alive(self, d: dict) -> bool:
        expire = d.get("expire_at")
        if expire is None:
            return True
        if isinstance(expire, str):
            expire = datetime.fromisoformat(expire)
        return expire > datetime.now(UTC)

    def _fold(self) -> dict[tuple, dict]:
        """Fold to the record per (task_id, step, execution_id) using the
        single executable merge definition (adjudicated v0.1.5)."""
        grouped: dict[tuple, list[dict]] = {}
        for d in self._rows:
            key = (
                d.get("task_id", ""),
                d.get("step", ""),
                d.get("execution_id", ""),
            )
            grouped.setdefault(key, []).append(d)
        return {
            key: record
            for key, seq in grouped.items()
            if (record := fold_snapshot_rows(seq)) is not None
        }

    @staticmethod
    def _latest_per_node(datas: list[dict]) -> list[dict]:
        """Latest generation per node (task_id, step)."""
        latest: dict[tuple, dict] = {}
        for d in datas:
            key = (d.get("task_id", ""), d.get("step", ""))
            cur = latest.get(key)
            if cur is None or str(d.get("created_at", "")) >= str(
                cur.get("created_at", "")
            ):
                latest[key] = d
        return list(latest.values())

    @staticmethod
    def _validate_snapshot_sort(sort: Sort | None) -> Sort:
        sort = sort or Sort()
        if sort.field not in SORT_FIELDS["snapshot"]:
            raise InvalidQueryError(
                f"sort.field {sort.field!r} outside the snapshot mechanism "
                f"whitelist: {sorted(SORT_FIELDS['snapshot'])}"
            )
        return sort

    @staticmethod
    def _sort_key(data: dict, field: str):
        """Mechanism sort key. No-expiry sorts as infinitely far (ASC last,
        DESC first) — the string "None" must never join the ordering."""
        value = data.get(field)
        if field == "expire_at":
            return (value is None, str(value) if value is not None else "")
        return str(value or "")

    async def get(
        self, task_id: str, step: str, *, execution_id: str | None = None
    ) -> TaskSnapshot | None:
        folded = self._fold()
        candidates = [
            d
            for (t, st, e), d in folded.items()
            if t == task_id and st == step
            and (execution_id is None or e == execution_id)
            and self._alive(d)
        ]
        if not candidates:
            return None
        candidates.sort(key=lambda d: str(d.get("created_at", "")))
        return TaskSnapshot.model_validate(candidates[-1])

    async def get_tree(
        self,
        root_task_id: str,
        *,
        max_depth: int | None = None,
        latest_only: bool = True,
    ) -> list[TaskSnapshot]:
        folded = self._fold()
        alive = [d for d in folded.values() if self._alive(d)]
        by_parent: dict[str, list[dict]] = {}
        for d in alive:
            parent = d.get("parent_task_id")
            if parent is not None:
                by_parent.setdefault(parent, []).append(d)

        depth_limit = max_depth if max_depth is not None else 32
        out: list[dict] = []
        visited: set[str] = set()

        def walk(task_id: str, depth: int) -> None:
            if depth > depth_limit or task_id in visited:
                return
            visited.add(task_id)
            out.extend(d for d in alive if d.get("task_id") == task_id)
            for child in by_parent.get(task_id, []):
                walk(child["task_id"], depth + 1)

        walk(root_task_id, 0)
        if latest_only:
            out = self._latest_per_node(out)
        return [TaskSnapshot.model_validate(d) for d in out]

    async def query(
        self,
        *,
        status: str | None = None,
        parent_task_id: str | None = None,
        time_range: TimeRange | None = None,
        page: Page | None = None,
        sort: Sort | None = None,
        **kwargs: Any,
    ) -> list[TaskSnapshot]:
        """Query snapshots by mechanism fields (latest generations only)."""
        sort = self._validate_snapshot_sort(sort)
        folded = self._fold()
        alive = self._latest_per_node(
            [d for d in folded.values() if self._alive(d)]
        )
        if status is not None:
            alive = [d for d in alive if d.get("status") == status]
        if parent_task_id is not None:
            alive = [
                d for d in alive if d.get("parent_task_id") == parent_task_id
            ]
        if time_range is not None:
            if time_range.start is not None:
                alive = [
                    d
                    for d in alive
                    if (dt := _parse_dt(d.get("created_at"))) is not None
                    and dt >= time_range.start
                ]
            if time_range.end is not None:
                alive = [
                    d
                    for d in alive
                    if (dt := _parse_dt(d.get("created_at"))) is not None
                    and dt < time_range.end
                ]
        alive.sort(
            key=lambda d: self._sort_key(d, sort.field),
            reverse=sort.direction is SortDirection.DESC,
        )
        page = page or Page()
        return [
            TaskSnapshot.model_validate(d)
            for d in alive[page.offset: page.offset + page.limit]
        ]


    async def aggregate(
        self,
        *,
        group_by: str,
        parent_task_id: str | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Aggregate snapshots by a mechanism field (latest generations)."""
        if group_by not in GROUP_BY_FIELDS["snapshot"]:
            raise InvalidQueryError(
                f"group_by {group_by!r} outside the snapshot mechanism "
                f"whitelist: {sorted(GROUP_BY_FIELDS['snapshot'])}"
            )
        folded = self._fold()
        alive = self._latest_per_node(
            [d for d in folded.values() if self._alive(d)]
        )
        if parent_task_id is not None:
            alive = [
                d for d in alive if d.get("parent_task_id") == parent_task_id
            ]
        out: dict[str, Any] = {}
        for d in alive:
            gval = str(d.get(group_by, "unknown"))
            bucket = out.setdefault(gval, {"count": 0, "cost": {}})
            bucket["count"] += 1
            for k, v in (d.get("cost") or {}).items():
                bucket["cost"][k] = bucket["cost"].get(k, 0.0) + v
        return out

class DependencyView:
    """Query surface over dependency edges (consumer profile)."""

    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows

    @property
    def capabilities(self) -> CapabilitySet:
        return CapabilitySet(dependency_query=True)


    async def seed(self, fixtures: dict) -> None:
        """Load fixture payloads into the dependency view (idempotent).

        Conformance consumer hook: fixtures["edges"] is a list of payload
        dicts; registered_at arrives as an ISO string.
        """
        for raw in fixtures.get("edges", []):
            data = dict(raw)
            if isinstance(data.get("registered_at"), str):
                data["registered_at"] = datetime.fromisoformat(
                    data["registered_at"]
                )
            self._rows.append(data)

    def _edges(self) -> list[DependencyEdge]:
        return [DependencyEdge.model_validate(d) for d in self._rows]

    async def read_graph(self, root_task_id: str) -> DependencyGraph:
        edges = self._edges()
        adjacency: dict[str, set[str]] = {}
        for e in edges:
            adjacency.setdefault(e.child_id, set()).add(e.parent_id)
            adjacency.setdefault(e.parent_id, set()).add(e.child_id)
        visited = {root_task_id}
        from collections import deque

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

    async def children_of(self, parent_task_id: str) -> list[str]:
        return sorted(
            e.child_id for e in self._edges() if e.parent_id == parent_task_id
        )

    async def parents_of(self, child_task_id: str) -> list[str]:
        return sorted(
            e.parent_id for e in self._edges() if e.child_id == child_task_id
        )

    async def all_edges(self) -> list[DependencyEdge]:
        return self._edges()


class AuditView:
    """Query surface over audit rows (consumer profile)."""

    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows

    @property
    def capabilities(self) -> CapabilitySet:
        return CapabilitySet(audit_query=True)

    async def query(
        self,
        *,
        task_id: str | None = None,
        scope: str | None = None,
        event_type: str | None = None,
        time_range: TimeRange | None = None,
        page: Page | None = None,
        sort: Sort | None = None,
        **kwargs: Any,
    ) -> list[AuditEvent]:
        """Query audit events by mechanism fields."""
        rows = [d for d in self._rows]
        if task_id is not None:
            rows = [d for d in rows if d.get("task_id") == task_id]
        if scope is not None:
            rows = [d for d in rows if d.get("scope") == scope]
        if event_type is not None:
            rows = [d for d in rows if d.get("event_type") == event_type]
        if time_range is not None:
            if time_range.start is not None:
                rows = [
                    d
                    for d in rows
                    if (dt := _parse_dt(d.get("created_at"))) is not None
                    and dt >= time_range.start
                ]
            if time_range.end is not None:
                rows = [
                    d
                    for d in rows
                    if (dt := _parse_dt(d.get("created_at"))) is not None
                    and dt < time_range.end
                ]

        sort = sort or Sort()
        if sort.field not in SORT_FIELDS["audit"]:
            raise InvalidQueryError(
                f"sort.field {sort.field!r} outside the audit mechanism "
                f"whitelist: {sorted(SORT_FIELDS['audit'])}"
            )
        rows.sort(
            key=lambda d: str(d.get(sort.field, "")),
            reverse=sort.direction is SortDirection.DESC,
        )
        page = page or Page()
        return [
            AuditEvent.model_validate(d)
            for d in rows[page.offset: page.offset + page.limit]
        ]

class ResultView:
    """Query surface over result manifests (consumer profile)."""

    def __init__(self, results: dict[str, dict]) -> None:
        self._results = results

    @property
    def capabilities(self) -> CapabilitySet:
        return CapabilitySet(result_query=True)

    async def get(self, stream_id: str) -> dict[str, Any] | None:
        doc = self._results.get(stream_id)
        if doc is None:
            return None
        expire = doc.get("expire_at")
        if expire is not None:
            if isinstance(expire, str):
                expire = datetime.fromisoformat(expire)
            if datetime.now(UTC) > expire:
                return None
        manifest = doc.get("manifest")
        return dict(manifest) if isinstance(manifest, dict) else None