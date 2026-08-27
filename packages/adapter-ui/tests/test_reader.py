"""Pinning tests for TraceBundleReader (consumer read over trace bundle)."""

import json

import pytest

from orditect.adapter.local import LocalFileStore
from orditect.adapter.ui import TraceBundleReader
from orditect.protocol import AuditEvent, DependencyEdge, TaskSnapshot

pytestmark = pytest.mark.unit


def _snap(tid, step, eid, status="", parent=None):
    return TaskSnapshot(
        task_id=tid, step=step, execution_id=eid,
        parent_task_id=parent, status=status,
    )


@pytest.fixture
async def bundle_dir(tmp_path):
    """Produce a trace bundle via LocalFileStore."""
    store = LocalFileStore(tmp_path / "bundle")
    await store.snapshot.save(_snap("root", "execute", "e1", "done"))
    await store.snapshot.save(_snap("a", "execute", "e1", "done", parent="root"))
    await store.snapshot.save(_snap("a", "execute", "e2", "running", parent="root"))
    await store.snapshot.save(_snap("b", "execute", "e1", "failed", parent="root"))
    await store.audit.append(
        AuditEvent(event_id="ev1", task_id="a", payload={"x": 1})
    )
    await store.dependency.write_dependency(
        DependencyEdge(child_id="a", parent_id="root", is_primary=True)
    )
    await store.dependency.write_dependency(
        DependencyEdge(child_id="b", parent_id="root")
    )
    from datetime import UTC, datetime, timedelta
    await store.result.save(
        "s1", {"data": "manifest"},
        expire_at=datetime.now(UTC) + timedelta(hours=1),
    )
    return tmp_path / "bundle"


class TestSnapshotRead:
    async def test_get_latest_generation(self, bundle_dir):
        reader = TraceBundleReader(bundle_dir)
        snap = await reader.snapshot.get("a", "execute")
        assert snap is not None
        assert snap.execution_id == "e2"  # latest generation

    async def test_get_tree_latest_only(self, bundle_dir):
        reader = TraceBundleReader(bundle_dir)
        tree = await reader.snapshot.get_tree("root", latest_only=True)
        ids = {(s.task_id, s.execution_id) for s in tree}
        assert ("a", "e2") in ids
        assert ("a", "e1") not in ids  # collapsed to latest

    async def test_get_tree_all_generations(self, bundle_dir):
        reader = TraceBundleReader(bundle_dir)
        tree = await reader.snapshot.get_tree("root", latest_only=False)
        ids = {(s.task_id, s.execution_id) for s in tree}
        assert ("a", "e1") in ids
        assert ("a", "e2") in ids

    async def test_query_by_status(self, bundle_dir):
        reader = TraceBundleReader(bundle_dir)
        rows = await reader.snapshot.query(status="failed")
        assert all(s.status == "failed" for s in rows)
        assert any(s.task_id == "b" for s in rows)

    async def test_aggregate(self, bundle_dir):
        reader = TraceBundleReader(bundle_dir)
        out = await reader.snapshot.aggregate(group_by="status")
        assert out.get("done", {}).get("count", 0) == 2  # root + a(e1)
        assert out.get("failed", {}).get("count", 0) == 1
        assert out.get("running", {}).get("count", 0) == 1


class TestDependencyRead:
    async def test_read_graph(self, bundle_dir):
        reader = TraceBundleReader(bundle_dir)
        graph = await reader.dependency.read_graph("root")
        assert {"root", "a", "b"} <= set(graph.task_ids)

    async def test_children_and_parents(self, bundle_dir):
        reader = TraceBundleReader(bundle_dir)
        assert set(await reader.dependency.children_of("root")) == {"a", "b"}
        assert await reader.dependency.parents_of("a") == ["root"]


class TestAuditRead:
    async def test_query_by_task(self, bundle_dir):
        reader = TraceBundleReader(bundle_dir)
        rows = await reader.audit.query(task_id="a")
        assert len(rows) == 1
        assert rows[0].event_id == "ev1"


class TestResultRead:
    async def test_get_manifest(self, bundle_dir):
        reader = TraceBundleReader(bundle_dir)
        manifest = await reader.result.get("s1")
        assert manifest is not None
        assert manifest["data"] == "manifest"

    async def test_get_missing(self, bundle_dir):
        reader = TraceBundleReader(bundle_dir)
        assert await reader.result.get("ghost") is None