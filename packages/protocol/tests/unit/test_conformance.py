"""B7 self-tests: the conformance kit must go green on a conforming fake and
red on a deliberately violating fake (CF-ALL-002 meta-pinning)."""

from __future__ import annotations

from typing import Any

import pytest

from orditect.protocol import CapabilitySet
from orditect.protocol.conformance import run_conformance
from orditect.protocol.errors import (
    IdempotencyConflictError,
    TerminalStateViolationError,
)
from orditect.protocol.models import AuditEvent, TaskPointer, TaskSnapshot


class _ContentPart:
    def __init__(self) -> None:
        self._content: dict[str, tuple[bytes, dict[str, Any]]] = {}

    async def put(self, content: bytes, **kw: Any) -> TaskPointer:
        key = f"mem://{len(self._content)}"
        self._content[key] = (content, kw.get("metadata") or {})
        return TaskPointer(backend="mem", key=key)

    async def get(self, pointer: TaskPointer) -> bytes:
        return self._content[pointer.key][0]

    async def delete(self, pointer: TaskPointer) -> bool:
        return self._content.pop(pointer.key, None) is not None

    async def exists(self, pointer: TaskPointer) -> bool:
        return pointer.key in self._content

    async def get_metadata(self, pointer: TaskPointer) -> dict[str, Any]:
        return self._content[pointer.key][1]


class _AuditPart:
    def __init__(self) -> None:
        self._audit: dict[str, AuditEvent] = {}

    async def append(self, event: AuditEvent) -> None:
        existing = self._audit.get(event.event_id)
        if existing is not None and existing.payload != event.payload:
            raise IdempotencyConflictError(event.event_id)
        self._audit[event.event_id] = event

    async def query(self, **kw: Any) -> list[AuditEvent]:
        tid = kw.get("task_id")
        return [e for e in self._audit.values() if tid is None or e.task_id == tid]


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
        if key in self._terminal and snapshot.status != self._snap[key].status:
            raise TerminalStateViolationError(str(key))
        self._snap[key] = snapshot

    async def save_terminal(self, snapshot: TaskSnapshot) -> None:
        key = (snapshot.task_id, snapshot.step, snapshot.execution_id)
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
        return [
            s for (t, st, _), s in self._snap.items()
            if t == task_id and st == step
        ]

    async def get_tree(self, root_task_id: str, **kw: Any) -> list[TaskSnapshot]:
        return list(self._snap.values())

    async def list_children(self, parent_task_id: str, **kw: Any) -> list[TaskSnapshot]:
        return [s for s in self._snap.values() if s.parent_task_id == parent_task_id]

    async def get_ancestors(self, task_id: str, **kw: Any) -> list[TaskSnapshot]:
        return []

    async def query(self, **kw: Any) -> list[TaskSnapshot]:
        return list(self._snap.values())

    async def aggregate(self, **kw: Any) -> dict[str, Any]:
        return {}


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
        )
        self._content = _ContentPart()
        self._audit = _AuditPart()
        self._result = _ResultPart()
        self._snapshot = _SnapshotPart()

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
        # audit query when task_id-only and no snapshot-specific kwargs
        # conformance uses task_id for audit; snapshot query uses status/parent_task_id/etc.
        if any(k in kw for k in ("status", "parent_task_id", "time_range", "sort", "page", "scope", "event_type")):
            if "scope" in kw or "event_type" in kw:
                return await self._audit.query(**kw)
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