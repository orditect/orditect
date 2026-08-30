
"""ManifestResolver 单测。"""
import asyncio

import pytest

from orditect.stream.client import ManifestResolver


class TestManifestResolver:
    async def test_resolve_tf_success(self):
        async def tf_query(task_id):
            return {"status": "succeeded", "result": {"url": "real.jpg"}}

        resolver = ManifestResolver(taskflow_query=tf_query, poll_interval=0.01)
        manifest = {
            "placeholders": [
                {"placeholder_id": "ph_1", "task_ref": "tf:task-1", "state": "pending"},
            ]
        }
        results = {}

        async def cb(pid, url):
            results[pid] = url

        await resolver.resolve_all(manifest, cb)
        assert results["ph_1"] == "real.jpg"

    async def test_resolve_tf_failed(self):
        async def tf_query(task_id):
            return {"status": "failed"}

        resolver = ManifestResolver(taskflow_query=tf_query, poll_interval=0.01)
        manifest = {
            "placeholders": [{"placeholder_id": "ph_1", "task_ref": "tf:task-1", "state": "pending"}]
        }
        results = {}

        async def cb(pid, url):
            results[pid] = url

        await resolver.resolve_all(manifest, cb)
        assert results["ph_1"] is None



    async def test_no_pending_noop(self):
        resolver = ManifestResolver()
        called = []

        async def cb(pid, url):
            called.append(pid)

        await resolver.resolve_all({"placeholders": []}, cb)
        assert called == []

class TestQueryFaultTolerance:
    """v0.1.7 pinning (issue #3): exceptions from the injected query
    function must not abort resolution.

    Red before: orchestrator.get_task raises TaskNotFoundError for a task
    that does not exist yet (its documented contract). A resolver polling
    before the enrich task was submitted crashed through _poll_task and
    failed the whole resolve_all gather.
    """

    async def test_not_found_exception_then_success(self):
        """A query that raises TaskNotFoundError while the task does not
        exist yet, then succeeds, resolves normally."""
        from orditect.flow.exceptions import TaskNotFoundError

        calls: list[str] = []

        async def tf_query(task_id: str):
            calls.append(task_id)
            if len(calls) < 3:
                raise TaskNotFoundError(f"task_id not found: {task_id}")
            return {"status": "succeeded", "result": {"url": "real.jpg"}}

        resolver = ManifestResolver(taskflow_query=tf_query, poll_interval=0.01)
        manifest = {
            "placeholders": [
                {"placeholder_id": "ph_1", "task_ref": "tf:enrich-ph_1", "state": "pending"},
            ]
        }
        results: dict = {}

        async def cb(pid, url):
            results[pid] = url

        await resolver.resolve_all(manifest, cb)
        assert results["ph_1"] == "real.jpg"
        assert len(calls) >= 3

    async def test_one_placeholder_exception_does_not_block_others(self):
        """A transiently-failing placeholder must not take the other
        placeholders down with it (gather isolation)."""
        from orditect.flow.exceptions import TaskNotFoundError

        async def tf_query(task_id: str):
            if task_id == "enrich-ph_late":
                raise TaskNotFoundError("not yet")
            return {"status": "succeeded", "result": {"url": f"{task_id}.jpg"}}

        resolver = ManifestResolver(
            taskflow_query=tf_query, poll_interval=0.01, max_wait=0.05
        )
        manifest = {
            "placeholders": [
                {"placeholder_id": "ph_late", "task_ref": "tf:enrich-ph_late", "state": "pending"},
                {"placeholder_id": "ph_ok", "task_ref": "tf:enrich-ph_ok", "state": "pending"},
            ]
        }
        results: dict = {}

        async def cb(pid, url):
            results[pid] = url

        await resolver.resolve_all(manifest, cb)
        assert results["ph_ok"] == "enrich-ph_ok.jpg"
        assert results["ph_late"] is None  # timed out, not crashed