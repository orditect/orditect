"""B2 pinning tests: data model semantics."""

from datetime import UTC, datetime, timedelta
import pytest
from pydantic import ValidationError

from orditect.protocol.models import (
    AuditEvent,
    Page,
    Sort,
    SortDirection,
    TaskPointer,
    TaskSnapshot,
    TimeRange,
    DependencyEdge,
    DependencyGraph,
)


@pytest.mark.unit
class TestTaskPointer:
    def test_minimal(self):
        p = TaskPointer(backend="postgres", key="task_records/row_id=1")
        assert p.backend == "postgres"
        assert p.metadata is None

    def test_metadata_omitted_when_none(self):
        p = TaskPointer(backend="s3", key="s3://b/k")
        assert "metadata" not in p.to_payload()

    def test_frozen(self):
        p = TaskPointer(backend="s3", key="k")
        with pytest.raises(ValidationError):
            p.key = "other"  # type: ignore[misc]


@pytest.mark.unit
class TestTaskSnapshot:
    def test_execution_id_required(self):
        with pytest.raises(ValidationError):
            TaskSnapshot(task_id="t1", step="s1")  # type: ignore[call-arg]

    def test_status_is_opaque_string(self):
        snap = TaskSnapshot(task_id="t1", step="s1", execution_id="e1", status="custom_word")
        assert snap.status == "custom_word"

    def test_parent_lineage_field(self):
        snap = TaskSnapshot(
            task_id="child", step="s", execution_id="e1", parent_task_id="parent"
        )
        assert snap.parent_task_id == "parent"

    def test_expire_at_absolute_instant(self):
        future = datetime.now(UTC) + timedelta(hours=1)
        snap = TaskSnapshot(task_id="t", step="s", execution_id="e", expire_at=future)
        assert snap.expire_at == future

    def test_none_fields_omitted_in_payload(self):
        snap = TaskSnapshot(task_id="t", step="s", execution_id="e")
        payload = snap.to_payload()
        assert "parent_task_id" not in payload
        assert "input_pointer" not in payload
        assert "error" not in payload
        # Required fields always present
        assert payload["task_id"] == "t"
        assert payload["execution_id"] == "e"


@pytest.mark.unit
class TestAuditEvent:
    def test_event_id_required(self):
        with pytest.raises(ValidationError):
            AuditEvent(task_id="t1")  # type: ignore[call-arg]

    def test_defaults(self):
        # FLIP(v0.1.2): AuditEvent.timestamp renamed to created_at — WI-1.4
        # unified mechanism time-field vocabulary (created_at/updated_at/expire_at)
        e = AuditEvent(event_id="ev1", task_id="t1")
        assert e.payload == {}
        assert e.event_type == ""
        assert e.created_at.tzinfo is not None  # T7: aware UTC default

@pytest.mark.unit
class TestQueryModels:
    def test_page_defaults(self):
        page = Page()
        assert page.limit == 100
        assert page.offset == 0

    def test_page_validation(self):
        with pytest.raises(ValidationError):
            Page(limit=0)
        with pytest.raises(ValidationError):
            Page(offset=-1)

    def test_sort_defaults(self):
        s = Sort()
        assert s.field == "created_at"
        assert s.direction is SortDirection.DESC

    def test_time_range_unbounded(self):
        tr = TimeRange()
        assert tr.start is None
        assert tr.end is None

@pytest.mark.unit
class TestDependencyEdge:
    def test_minimal(self):
        e = DependencyEdge(child_id="c", parent_id="p")
        assert e.is_primary is False
        assert e.registered_at.tzinfo is not None  # T7: aware UTC default

    def test_primary_flag(self):
        e = DependencyEdge(child_id="c", parent_id="p", is_primary=True)
        assert e.is_primary is True

    def test_frozen(self):
        e = DependencyEdge(child_id="c", parent_id="p")
        with pytest.raises(ValidationError):
            e.child_id = "other"  # type: ignore[misc]

    def test_payload_key_set(self):
        e = DependencyEdge(child_id="c", parent_id="p")
        assert set(e.to_payload().keys()) == {
            "child_id", "parent_id", "is_primary", "registered_at",
        }

    def test_self_loop_is_data_not_rejected(self):
        """T12: the store records facts; a self-loop is data, not an error."""
        e = DependencyEdge(child_id="a", parent_id="a")
        assert e.child_id == e.parent_id == "a"


@pytest.mark.unit
class TestDependencyGraph:
    def test_defaults(self):
        g = DependencyGraph(root_task_id="root")
        assert g.task_ids == []
        assert g.edges == []

    def test_payload_key_set(self):
        g = DependencyGraph(
            root_task_id="root",
            task_ids=["root", "a"],
            edges=[DependencyEdge(child_id="a", parent_id="root")],
        )
        payload = g.to_payload()
        assert set(payload.keys()) == {"root_task_id", "task_ids", "edges"}
        assert payload["task_ids"] == ["root", "a"]
        assert len(payload["edges"]) == 1