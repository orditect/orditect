"""B7 self-tests: the conformance kit must go green on a conforming fake and
red on a deliberately violating fake (CF-ALL-002 meta-pinning)."""

from __future__ import annotations

from typing import Any

import pytest

from orditect.protocol import CapabilitySet
from orditect.protocol.conformance import run_conformance
from orditect.protocol.errors import (
    IdempotencyConflictError,
    InvalidQueryError,
    TerminalStateViolationError,
)
from orditect.protocol.models import (
    AuditEvent,
    DependencyEdge,
    DependencyGraph,
    TaskPointer,
    TaskSnapshot,
)
from orditect.protocol.mechanism import GROUP_BY_FIELDS, SORT_FIELDS


class _ContentPart:
    def __init__(self) -> None:
        self._content: dict[str, tuple[bytes, dict[str, Any]]] = {}

    async def put(self, content: bytes, **kw: Any) -> TaskPointer:
        meta = dict(kw.get("metadata") or {})
        if kw.get("content_type") is not None:
            meta["content_type"] = kw["content_type"]
        key = f"mem://{len(self._content)}"
        self._content[key] = (content, meta)
        return TaskPointer(backend="mem", key=key)

    async def get(self, pointer: TaskPointer) -> bytes:
        from orditect.protocol.errors import ContentNotFoundError
        try:
            return self._content[pointer.key][0]
        except KeyError:
            raise ContentNotFoundError(pointer.key) from None

    async def delete(self, pointer: TaskPointer) -> bool:
        return self._content.pop(pointer.key, None) is not None

    async def exists(self, pointer: TaskPointer) -> bool:
        return pointer.key in self._content

    async def get_metadata(self, pointer: TaskPointer) -> dict[str, Any]:
        from orditect.protocol.errors import ContentNotFoundError
        try:
            return dict(self._content[pointer.key][1])
        except KeyError:
            raise ContentNotFoundError(pointer.key) from None


class _AuditPart:
    def __init__(self) -> None:
        self._audit: dict[str, AuditEvent] = {}

    async def append(self, event: AuditEvent) -> None:
        existing = self._audit.get(event.event_id)
        if existing is not None and existing.payload != event.payload:
            raise IdempotencyConflictError(event.event_id)
        self._audit[event.event_id] = event

    async def query(self, **kw: Any) -> list[AuditEvent]:
        sort = kw.get("sort")
        if sort is not None and sort.field not in SORT_FIELDS["audit"]:
            raise InvalidQueryError(f"sort.field {sort.field!r} not whitelisted")
        tid = kw.get("task_id")
        rows = [e for e in self._audit.values() if tid is None or e.task_id == tid]
        tr = kw.get("time_range")
        if tr is not None:
            if tr.start is not None:
                rows = [e for e in rows if e.created_at >= tr.start]
            if tr.end is not None:
                rows = [e for e in rows if e.created_at < tr.end]
        from orditect.protocol.models import Sort as _Sort, SortDirection
        sort = sort or _Sort()
        rows.sort(key=lambda e: getattr(e, sort.field),
                  reverse=sort.direction is SortDirection.DESC)
        page = kw.get("page")
        if page is not None:
            rows = rows[page.offset: page.offset + page.limit]
        return rows


class _ResultPart:
    def __init__(self) -> None:
        self._result: dict[str, tuple[dict[str, Any], Any]] = {}

    async def save(self, stream_id: str, manifest: dict[str, Any], **kw: Any) -> None:
        self._result[stream_id] = (manifest, kw["expire_at"])

    async def get(self, stream_id: str) -> dict[str, Any] | None:
        from datetime import UTC, datetime
        item = self._result.get(stream_id)
        if item is None:
            return None
        manifest, exp = item
        return None if datetime.now(UTC) > exp else manifest


class _SnapshotPart:
    def __init__(self) -> None:
        self._snap: dict[tuple[str, str, str], TaskSnapshot] = {}
        self._terminal: set[tuple[str, str, str]] = set()

    async def save(self, snapshot: TaskSnapshot) -> None:
        key = (snapshot.task_id, snapshot.step, snapshot.execution_id)
        existing = self._snap.get(key)
        if key in self._terminal and existing is not None:
            if snapshot.status != existing.status:
                raise TerminalStateViolationError(str(key))
        if existing is not None:
            updates: dict = {
                f: getattr(snapshot, f)
                for f in (
                    "parent_task_id", "input_pointer", "output_pointer",
                    "error", "cost", "model", "expire_at",
                )
                if getattr(snapshot, f) is not None
            }
            # Status advances only when non-empty (state never regresses).
            if snapshot.status:
                updates["status"] = snapshot.status
            updates["updated_at"] = snapshot.updated_at
            merged = existing.model_copy(update=updates)
            self._snap[key] = merged
            return
        self._snap[key] = snapshot

    async def save_terminal(self, snapshot: TaskSnapshot) -> None:
        key = (snapshot.task_id, snapshot.step, snapshot.execution_id)
        existing = self._snap.get(key)
        if key in self._terminal and existing is not None:
            # T4: identical business content (mechanism clock fields
            # excluded) is a silent dedup; differing content conflicts.
            from orditect.protocol.mechanism import idempotent_payload_equal
            if not idempotent_payload_equal(
                existing.model_dump(), snapshot.model_dump()
            ):
                raise TerminalStateViolationError(str(key))
            return
        self._snap[key] = snapshot
        self._terminal.add(key)

    async def get(self, task_id: str, step: str, **kw: Any) -> TaskSnapshot | None:
        from datetime import UTC, datetime
        rows = [
            s for (t, st, _), s in self._snap.items()
            if t == task_id and st == step
            and (s.expire_at is None or s.expire_at > datetime.now(UTC))
        ]
        return rows[-1] if rows else None

    async def list_versions(self, task_id: str, step: str, **kw: Any) -> list[TaskSnapshot]:
        sort = kw.get("sort")
        if sort is not None and sort.field not in SORT_FIELDS["snapshot"]:
            raise InvalidQueryError(f"sort.field {sort.field!r} not whitelisted")
        rows = [
            s for (t, st, _), s in self._snap.items()
            if t == task_id and st == step
        ]
        from orditect.protocol.models import Sort as _Sort, SortDirection
        sort = sort or _Sort()
        rows.sort(key=lambda s: getattr(s, sort.field),
                  reverse=sort.direction is SortDirection.DESC)
        return rows

    async def get_tree(self, root_task_id: str, **kw: Any) -> list[TaskSnapshot]:
        return list(self._snap.values())

    async def list_children(self, parent_task_id: str, **kw: Any) -> list[TaskSnapshot]:
        return [s for s in self._snap.values() if s.parent_task_id == parent_task_id]

    async def get_ancestors(self, task_id: str, **kw: Any) -> list[TaskSnapshot]:
        return []

    async def query(self, **kw: Any) -> list[TaskSnapshot]:
        sort = kw.get("sort")
        if sort is not None and sort.field not in SORT_FIELDS["snapshot"]:
            raise InvalidQueryError(f"sort.field {sort.field!r} not whitelisted")
        rows = list(self._snap.values())
        if kw.get("status") is not None:
            rows = [s for s in rows if s.status == kw["status"]]
        if kw.get("parent_task_id") is not None:
            rows = [s for s in rows if s.parent_task_id == kw["parent_task_id"]]
        tr = kw.get("time_range")
        if tr is not None:
            if tr.start is not None:
                rows = [s for s in rows if s.created_at >= tr.start]
            if tr.end is not None:
                rows = [s for s in rows if s.created_at < tr.end]
        from orditect.protocol.models import Sort as _Sort, SortDirection
        sort = sort or _Sort()
        rows.sort(key=lambda s: getattr(s, sort.field),
                  reverse=sort.direction is SortDirection.DESC)
        page = kw.get("page")
        if page is not None:
            rows = rows[page.offset: page.offset + page.limit]
        return rows

    async def aggregate(self, **kw: Any) -> dict[str, Any]:
        group_by = kw.get("group_by")
        if group_by not in GROUP_BY_FIELDS["snapshot"]:
            raise InvalidQueryError(f"group_by {group_by!r} not whitelisted")
        rows = list(self._snap.values())
        if kw.get("parent_task_id") is not None:
            rows = [s for s in rows if s.parent_task_id == kw["parent_task_id"]]
        out: dict[str, Any] = {}
        for s in rows:
            gval = str(getattr(s, group_by, "unknown"))
            bucket = out.setdefault(gval, {"count": 0, "cost": {}})
            bucket["count"] += 1
            for k, v in (s.cost or {}).items():
                bucket["cost"][k] = bucket["cost"].get(k, 0.0) + v
        return out

class _DependencyPart:
    def __init__(self) -> None:
        self._edges: dict[tuple[str, str], DependencyEdge] = {}

    async def write_dependency(self, edge: DependencyEdge) -> None:
        key = (edge.child_id, edge.parent_id)
        existing = self._edges.get(key)
        if existing is None:
            self._edges[key] = edge
            return
        if existing.is_primary != edge.is_primary:
            raise IdempotencyConflictError(str(key))

    async def read_graph(self, root_task_id: str) -> DependencyGraph:
        from collections import deque
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

class _GoodAdapter:
    """Composite of four per-domain parts (dispatch by call signature).

    This mirrors real adapter structure: each domain is a self-contained
    part; the composite routes calls and exposes one CapabilitySet.
    """

    def __init__(self) -> None:
        self._caps = CapabilitySet(
            content_sink=True, content_query=True,
            audit_sink=True, audit_query=True,
            result_sink=True, result_query=True,
            snapshot_sink=True, snapshot_query=True,
            dependency_sink=True, dependency_query=True,
        )
        self._content = _ContentPart()
        self._audit = _AuditPart()
        self._result = _ResultPart()
        self._snapshot = _SnapshotPart()
        self._dependency = _DependencyPart()

    @property
    def capabilities(self) -> CapabilitySet:
        return self._caps

    # --- dispatch: route by argument shape (duck-typed) ---
    async def put(self, content: bytes, **kw: Any) -> TaskPointer:
        return await self._content.put(content, **kw)

    async def delete(self, pointer: TaskPointer) -> bool:
        return await self._content.delete(pointer)

    async def exists(self, pointer: TaskPointer) -> bool:
        return await self._content.exists(pointer)

    async def get_metadata(self, pointer: TaskPointer) -> dict[str, Any]:
        return await self._content.get_metadata(pointer)

    async def append(self, event: AuditEvent) -> None:
        return await self._audit.append(event)

    async def save_terminal(self, snapshot: TaskSnapshot) -> None:
        return await self._snapshot.save_terminal(snapshot)

    # Disambiguated by argument types / counts:
    async def get(self, *args: Any, **kw: Any):
        # content: get(pointer); snapshot: get(task_id, step); result: get(stream_id)
        if args and isinstance(args[0], TaskPointer):
            return await self._content.get(*args, **kw)
        if len(args) >= 2:
            return await self._snapshot.get(*args, **kw)
        return await self._result.get(*args, **kw)

    async def save(self, *args: Any, **kw: Any) -> None:
        # result: save(stream_id, manifest, expire_at=...); snapshot: save(snapshot)
        if args and isinstance(args[0], TaskSnapshot):
            return await self._snapshot.save(*args, **kw)
        return await self._result.save(*args, **kw)

    async def query(self, **kw: Any):
        # snapshot query is the only one accepting status/parent_task_id;
        # audit query is identified by scope/event_type or by task_id alone.
        if "status" in kw or "parent_task_id" in kw:
            return await self._snapshot.query(**kw)
        return await self._audit.query(**kw)

    async def list_versions(self, task_id: str, step: str, **kw: Any) -> list[TaskSnapshot]:
        return await self._snapshot.list_versions(task_id, step, **kw)

    async def get_tree(self, root_task_id: str, **kw: Any) -> list[TaskSnapshot]:
        return await self._snapshot.get_tree(root_task_id, **kw)

    async def list_children(self, parent_task_id: str, **kw: Any) -> list[TaskSnapshot]:
        return await self._snapshot.list_children(parent_task_id, **kw)

    async def get_ancestors(self, task_id: str, **kw: Any) -> list[TaskSnapshot]:
        return await self._snapshot.get_ancestors(task_id, **kw)

    async def aggregate(self, **kw: Any) -> dict[str, Any]:
        return await self._snapshot.aggregate(**kw)

    # --- dependency domain (no name collisions: unique method names) ---
    async def write_dependency(self, edge: DependencyEdge) -> None:
        return await self._dependency.write_dependency(edge)

    async def read_graph(self, root_task_id: str) -> DependencyGraph:
        return await self._dependency.read_graph(root_task_id)

    async def all_edges(self) -> list[DependencyEdge]:
        return await self._dependency.all_edges()

    async def children_of(self, parent_task_id: str) -> list[str]:
        return await self._dependency.children_of(parent_task_id)

    async def parents_of(self, child_task_id: str) -> list[str]:
        return await self._dependency.parents_of(child_task_id)

class _BadAuditAdapter:
    """Deliberately violates T4: allows same event_id with different payload."""

    def __init__(self) -> None:
        self._audit: dict[str, AuditEvent] = {}

    @property
    def capabilities(self) -> CapabilitySet:
        return CapabilitySet(audit_sink=True, audit_query=True)

    async def append(self, event: AuditEvent) -> None:
        self._audit[event.event_id] = event  # silently overwrites (violation)

    async def query(self, **kw: Any) -> list[AuditEvent]:
        return list(self._audit.values())


@pytest.mark.unit
class TestConformanceKit:
    def test_good_adapter_all_pass_or_skip(self):
        report = run_conformance(_GoodAdapter())
        assert report.failed == 0, report.summary()
        assert report.passed > 0

    def test_bad_adapter_turns_suite_red(self):
        """CF-ALL-002 (T8/T4): a violating implementation must fail the suite."""
        report = run_conformance(_BadAuditAdapter())
        assert report.failed > 0, "suite must detect the T4 violation"
        failed_ids = {r.case_id for r in report.results if r.status == "failed"}
        assert "CF-AUD-002" in failed_ids

    def test_undeclared_half_domain_skips(self):
        """CF-ALL-001 (T8): undeclared half-domains skip, never fail."""
        class _EmptyAdapter:
            @property
            def capabilities(self) -> CapabilitySet:
                return CapabilitySet()  # nothing declared

        report = run_conformance(_EmptyAdapter())
        assert report.failed == 0
        assert report.skipped > 0
        assert report.passed == 0

# ---------- M3: profile tiers ----------

class _SinkOnlySnapshotAdapter:
    """Declares snapshot_sink WITHOUT its pair (full-tier ineligible)."""

    def __init__(self) -> None:
        self._snap: dict = {}

    @property
    def capabilities(self) -> CapabilitySet:
        return CapabilitySet(snapshot_sink=True)

    async def save(self, snapshot: TaskSnapshot) -> None:
        key = (snapshot.task_id, snapshot.step, snapshot.execution_id)
        self._snap[key] = snapshot

    async def save_terminal(self, snapshot: TaskSnapshot) -> None:
        key = (snapshot.task_id, snapshot.step, snapshot.execution_id)
        self._snap[key] = snapshot


class _BridgeAdapter:
    """Producer shape: three sinks, no queries."""

    def __init__(self) -> None:
        self._caps = CapabilitySet(
            audit_sink=True, snapshot_sink=True, dependency_sink=True,
        )
        self._audit = _AuditPart()
        self._snapshot = _SnapshotPart()
        self._dependency = _DependencyPart()

    @property
    def capabilities(self) -> CapabilitySet:
        return self._caps

    async def append(self, event: AuditEvent) -> None:
        return await self._audit.append(event)

    async def save(self, snapshot: TaskSnapshot) -> None:
        return await self._snapshot.save(snapshot)

    async def save_terminal(self, snapshot: TaskSnapshot) -> None:
        return await self._snapshot.save_terminal(snapshot)

    async def write_dependency(self, edge: DependencyEdge) -> None:
        return await self._dependency.write_dependency(edge)


class _BridgeWithExtraQuery(_BridgeAdapter):
    """Producer that ALSO declares snapshot_query (minimum bar, not ceiling)."""

    def __init__(self) -> None:
        super().__init__()
        self._caps = CapabilitySet(
            audit_sink=True, snapshot_sink=True, snapshot_query=True,
            dependency_sink=True,
        )

    async def get(self, task_id: str, step: str, **kw: Any):
        return await self._snapshot.get(task_id, step, **kw)

    async def list_versions(self, task_id: str, step: str, **kw: Any):
        return await self._snapshot.list_versions(task_id, step, **kw)

    async def get_tree(self, root_task_id: str, **kw: Any):
        return await self._snapshot.get_tree(root_task_id, **kw)

    async def list_children(self, parent_task_id: str, **kw: Any):
        return await self._snapshot.list_children(parent_task_id, **kw)

    async def get_ancestors(self, task_id: str, **kw: Any):
        return await self._snapshot.get_ancestors(task_id, **kw)

    async def query(self, **kw: Any):
        return await self._snapshot.query(**kw)

    async def aggregate(self, **kw: Any):
        return await self._snapshot.aggregate(**kw)


class _ConsumerWithSeed:
    """Read-only consumer implementing the seed hook over memory parts."""

    def __init__(self) -> None:
        self._snap: dict[tuple, TaskSnapshot] = {}
        self._edges: dict[tuple, DependencyEdge] = {}

    @property
    def capabilities(self) -> CapabilitySet:
        return CapabilitySet(snapshot_query=True, dependency_query=True)

    async def seed(self, fixtures: dict) -> None:
        from datetime import datetime
        for raw in fixtures["snapshots"]:
            data = dict(raw)
            if isinstance(data.get("expire_at"), str):
                data["expire_at"] = datetime.fromisoformat(data["expire_at"])
            snap = TaskSnapshot(**data)
            self._snap[(snap.task_id, snap.step, snap.execution_id)] = snap
        for raw in fixtures["edges"]:
            edge = DependencyEdge(**raw)
            self._edges[(edge.child_id, edge.parent_id)] = edge

    # snapshot query surface
    async def get(self, task_id: str, step: str, **kw: Any):
        from datetime import UTC, datetime
        rows = [
            s for (t, st, _), s in self._snap.items()
            if t == task_id and st == step
            and (s.expire_at is None or s.expire_at > datetime.now(UTC))
        ]
        return rows[-1] if rows else None

    async def get_tree(self, root_task_id: str, **kw: Any):
        from datetime import UTC, datetime
        alive = [
            s for s in self._snap.values()
            if s.expire_at is None or s.expire_at > datetime.now(UTC)
        ]
        if kw.get("latest_only", True):
            latest: dict = {}
            for s in alive:
                if (s.task_id not in latest
                        or s.created_at >= latest[s.task_id].created_at):
                    latest[s.task_id] = s
            return list(latest.values())
        return alive

    async def query(self, **kw: Any):
        from datetime import UTC, datetime
        rows = [
            s for s in self._snap.values()
            if s.expire_at is None or s.expire_at > datetime.now(UTC)
        ]
        if kw.get("status") is not None:
            rows = [s for s in rows if s.status == kw["status"]]
        return rows

    async def aggregate(self, **kw: Any):
        from datetime import UTC, datetime
        group_by = kw.get("group_by", "status")
        rows = [
            s for s in self._snap.values()
            if s.expire_at is None or s.expire_at > datetime.now(UTC)
        ]
        latest: dict = {}
        for s in rows:
            if (s.task_id not in latest
                    or s.created_at >= latest[s.task_id].created_at):
                latest[s.task_id] = s
        out: dict = {}
        for s in latest.values():
            gval = str(getattr(s, group_by, "unknown"))
            bucket = out.setdefault(gval, {"count": 0, "cost": {}})
            bucket["count"] += 1
            for k, v in (s.cost or {}).items():
                bucket["cost"][k] = bucket["cost"].get(k, 0.0) + v
        return out

    # dependency query surface
    async def read_graph(self, root_task_id: str):
        from collections import deque
        adjacency: dict[str, set] = {}
        for c, p in self._edges:
            adjacency.setdefault(c, set()).add(p)
            adjacency.setdefault(p, set()).add(c)
        visited = {root_task_id}
        queue: deque = deque([root_task_id])
        while queue:
            node = queue.popleft()
            for nxt in adjacency.get(node, ()):
                if nxt not in visited:
                    visited.add(nxt)
                    queue.append(nxt)
        edges = [e for e in self._edges.values() if e.child_id in visited]
        return DependencyGraph(
            root_task_id=root_task_id, task_ids=sorted(visited), edges=edges,
        )

    async def children_of(self, parent_task_id: str):
        return sorted(c for (c, p) in self._edges if p == parent_task_id)

    async def parents_of(self, child_task_id: str):
        return sorted(p for (c, p) in self._edges if c == child_task_id)

    async def all_edges(self):
        return list(self._edges.values())


class _ConsumerNoSeed:
    @property
    def capabilities(self) -> CapabilitySet:
        return CapabilitySet(snapshot_query=True)


class _EmptyAdapter:
    @property
    def capabilities(self) -> CapabilitySet:
        return CapabilitySet()


@pytest.mark.unit
class TestFullProfileEligibility:
    def test_unpaired_declaration_ineligible(self):
        report = run_conformance(_SinkOnlySnapshotAdapter(), profile="full")
        assert report.eligibility_error is not None
        assert "paired" in report.eligibility_error
        assert report.results == []
        assert report.failed == 0  # eligibility is not a case failure
        assert "INELIGIBLE" in report.summary()

    def test_empty_adapter_is_eligible(self):
        """Zero declarations = no pairing violation; all cases skip."""
        report = run_conformance(_EmptyAdapter(), profile="full")
        assert report.eligibility_error is None
        assert report.failed == 0
        assert report.skipped > 0
        assert report.passed == 0


@pytest.mark.unit
class TestProducerProfile:
    def test_bridge_sinks_verified_queries_not_required(self):
        report = run_conformance(_BridgeAdapter(), profile="producer")
        assert report.eligibility_error is None
        assert report.failed == 0, report.summary()
        assert report.passed > 0
        # query half-domains were never declared: nothing failed on queries
        assert all(r.half_domain != "view" for r in report.results)

    def test_bad_bridge_still_caught(self):
        """A T4-violating producer goes red under the producer tier too."""
        report = run_conformance(_BadAuditAdapter(), profile="producer")
        assert report.failed > 0

    def test_extra_declared_query_still_verified(self):
        """Minimum bar, not ceiling: an additionally declared query half-domain
        is verified even under the producer profile."""
        report = run_conformance(_BridgeWithExtraQuery(), profile="producer")
        assert report.failed == 0, report.summary()
        # snapshot_sink cases ran (producer) — and snapshot_query being
        # declared means its guards executed inside those cases.
        assert report.passed > 0


@pytest.mark.unit
class TestConsumerProfile:
    def test_seeded_consumer_runs_view_cases(self):
        report = run_conformance(_ConsumerWithSeed(), profile="consumer")
        assert report.failed == 0, report.summary()
        view_results = [r for r in report.results if r.half_domain == "view"]
        assert len(view_results) == 4
        assert all(r.status == "passed" for r in view_results)

    def test_unseeded_consumer_degrades_to_skip(self):
        report = run_conformance(_ConsumerNoSeed(), profile="consumer")
        assert report.failed == 0
        view_results = [r for r in report.results if r.half_domain == "view"]
        assert len(view_results) == 4
        assert all(r.status == "skipped" for r in view_results)
        assert all("seed not implemented" in r.detail for r in view_results)

    def test_empty_adapter_consumer_all_skip(self):
        report = run_conformance(_EmptyAdapter(), profile="consumer")
        assert report.failed == 0
        assert report.passed == 0


@pytest.mark.unit
class TestProfileValidation:
    def test_unknown_profile_rejected(self):
        import pytest as _pytest
        with _pytest.raises(ValueError, match="unknown conformance profile"):
            run_conformance(_EmptyAdapter(), profile="bogus")

@pytest.mark.unit
class TestCaseRegistrationIntegrity:
    """Meta-pinning: every CF case is registered under the half-domain its
    id prefix implies (prevents silent domain mismatch like CF-SNP-011/012
    being registered as audit_sink and never executing)."""

    def test_case_domain_prefix_matches_registration(self):
        from orditect.protocol.conformance.runner import _all_cases

        prefix = {
            "CTT": "content_sink",
            "AUD": "audit_sink",
            "RST": "result_sink",
            "SNP": "snapshot_sink",
            "DEP": "dependency_sink",
            "VIEW": "view",
        }
        for c in _all_cases():
            code = c.case_id.split("-")[1]
            assert c.half_domain == prefix[code], (
                f"{c.case_id} registered as {c.half_domain}, "
                f"expected {prefix[code]}"
            )