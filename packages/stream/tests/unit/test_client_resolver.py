
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