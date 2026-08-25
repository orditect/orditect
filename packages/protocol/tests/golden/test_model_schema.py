"""Golden schema pinning: freeze the serialized key sets of all data models.

Discipline (aligned with the stream golden-test culture):
- Any model change (new field / rename / drop) must update these snapshots
  in the same commit and pass review.
- We pin the *key set* of to_payload() output, plus required-field presence.
"""

from datetime import datetime, timezone

import pytest

from orditect.protocol.models import (
    AuditEvent,
    Page,
    Sort,
    TaskPointer,
    TaskSnapshot,
    TimeRange,
)

pytestmark = pytest.mark.golden

_TS = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)


class TestTaskPointerSchema:
    def test_full_key_set(self):
        p = TaskPointer(backend="b", key="k", metadata={"m": 1})
        assert set(p.to_payload().keys()) == {"backend", "key", "metadata"}

    def test_minimal_key_set(self):
        p = TaskPointer(backend="b", key="k")
        assert set(p.to_payload().keys()) == {"backend", "key"}


class TestTaskSnapshotSchema:
    FULL_KEYS = {
        "task_id", "step", "execution_id", "parent_task_id", "status",
        "input_pointer", "output_pointer", "error", "cost", "model",
        "created_at", "updated_at", "expire_at",
    }

    def test_full_key_set(self):
        snap = TaskSnapshot(
            task_id="t", step="s", execution_id="e",
            parent_task_id="p", status="x",
            input_pointer=TaskPointer(backend="b", key="k"),
            output_pointer=TaskPointer(backend="b", key="k2"),
            error="err", cost={"usd": 0.1}, model="m",
            created_at=_TS, updated_at=_TS, expire_at=_TS,
        )
        assert set(snap.to_payload().keys()) == self.FULL_KEYS

    def test_minimal_key_set(self):
        snap = TaskSnapshot(
            task_id="t", step="s", execution_id="e",
            created_at=_TS, updated_at=_TS,
        )
        # status has a default "" so it is always present; optional None fields omitted
        assert set(snap.to_payload().keys()) == {
            "task_id", "step", "execution_id", "status", "created_at", "updated_at",
        }


class TestAuditEventSchema:
    def test_full_key_set(self):
        e = AuditEvent(
            event_id="ev", task_id="t", scope="sc", event_type="et",
            source="flow", payload={"a": 1}, timestamp=_TS,
        )
        assert set(e.to_payload().keys()) == {
            "event_id", "task_id", "scope", "event_type", "source",
            "payload", "timestamp",
        }


class TestQueryModelSchema:
    def test_page_keys(self):
        assert set(Page().to_payload().keys()) == {"limit", "offset"}

    def test_sort_keys(self):
        assert set(Sort().to_payload().keys()) == {"field", "direction"}

    def test_time_range_full(self):
        tr = TimeRange(start=_TS, end=_TS)
        assert set(tr.to_payload().keys()) == {"start", "end"}

    def test_time_range_empty(self):
        assert TimeRange().to_payload() == {}