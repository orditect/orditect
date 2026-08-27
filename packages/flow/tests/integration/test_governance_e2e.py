"""End-to-end dependency-governance tests against real Redis (v0.1.1).

Simulates an external orchestration system driving the full lifecycle:
register -> notify terminal -> readiness -> voting -> cancel -> recovery.
Requires Redis at redis://localhost:6379/15 (integration-suite convention).
"""

from __future__ import annotations

import pytest
import pytest_asyncio
from orditect.core import TaskRedisDB
from orditect.flow.storage.factory import (
    TASKFLOW_TERMINAL_STATUSES,
    TASKFLOW_TRANSITIONS,
)

from orditect.flow.governance import DependencyGovernor
from orditect.protocol import DependencyEdge, DependencyGraph

REDIS_URL = "redis://localhost:6379/15"

pytestmark = pytest.mark.integration


@pytest_asyncio.fixture
async def storage():
    instance = TaskRedisDB(
        redis_url=REDIS_URL,
        terminal_statuses=TASKFLOW_TERMINAL_STATUSES,
        transitions=TASKFLOW_TRANSITIONS,
    )
    await instance.connect()
    await instance.client.flushdb()
    yield instance
    await instance.client.flushdb()
    await instance.close()

class _Lifecycle:
    """Minimal lifecycle double: cancel = status -> cancelled (flow vocabulary)."""

    def __init__(self, storage) -> None:
        self._storage = storage
        self.cancelled: list[str] = []

    async def cancel(self, task_id: str) -> bool:
        self.cancelled.append(task_id)
        await self._storage.update_task(
            task_id, {"status": "cancelled"}, validate_status_transfer=False
        )
        return True


class _DepGraphStore:
    def __init__(self) -> None:
        self._edges: dict[tuple[str, str], DependencyEdge] = {}

    async def write_dependency(self, edge: DependencyEdge) -> None:
        self._edges[(edge.child_id, edge.parent_id)] = edge

    async def read_graph(self, root_id: str) -> DependencyGraph:
        from collections import deque
        adjacency: dict[str, set[str]] = {}
        for child_id, parent_id in self._edges:
            adjacency.setdefault(child_id, set()).add(parent_id)
            adjacency.setdefault(parent_id, set()).add(child_id)
        visited = {root_id}
        queue: deque[str] = deque([root_id])
        while queue:
            node = queue.popleft()
            for nxt in adjacency.get(node, ()):
                if nxt not in visited:
                    visited.add(nxt)
                    queue.append(nxt)
        edges = [e for e in self._edges.values() if e.child_id in visited]
        return DependencyGraph(root_task_id=root_id,
                               task_ids=sorted(visited), edges=edges)

    async def all_edges(self) -> list[DependencyEdge]:
        return list(self._edges.values())


def _gov(storage, lifecycle=None, store=None) -> DependencyGovernor:
    return DependencyGovernor(
        storage,
        success_words=frozenset({"succeeded"}),
        lifecycle=lifecycle,
        dep_graph_store=store,
    )


async def test_e2e_three_parent_readiness_and_dag_query(storage):
    """Full cycle: register 3 parents -> all succeed -> ready -> graph query."""
    store = _DepGraphStore()
    gov = _gov(storage, store=store)
    for tid in ("p1", "p2", "p3", "c"):
        await storage.initialize_task(tid, expiry=300, initial_status="pending")
    for tid in ("p1", "p2", "p3"):
        await storage.update_task(tid, {"status": "queued"})
        await storage.update_task(tid, {"status": "running"})

    await gov.register_dependency("c", ["p1", "p2", "p3"])
    assert await storage.get_remaining_deps("c") == 3
    assert await gov.get_ready_tasks() == []

    # external orchestrator closes parents one by one (its responsibility)
    await gov.notify_task_terminal("p1", "succeeded")
    await gov.notify_task_terminal("p2", "succeeded")
    assert await gov.get_ready_tasks() == []
    await gov.notify_task_terminal("p3", "succeeded")
    assert await gov.get_ready_tasks() == ["c"]

    # cold-path graph query
    graph = await gov.get_dependency_graph("c")
    assert len(graph.edges) == 3
    primaries = [e for e in graph.edges if e.is_primary]
    assert len(primaries) == 1 and primaries[0].parent_id == "p1"


async def test_e2e_failed_parent_cascades_cancel(storage):
    """All parents fail -> automatic votes -> child cancelled via lifecycle."""
    lifecycle = _Lifecycle(storage)
    gov = _gov(storage, lifecycle=lifecycle)
    for tid in ("p1", "p2", "c"):
        await storage.initialize_task(tid, expiry=300, initial_status="pending")
    for tid in ("p1", "p2"):
        await storage.update_task(tid, {"status": "queued"})
        await storage.update_task(tid, {"status": "running"})

    await gov.register_dependency("c", ["p1", "p2"])
    await gov.notify_task_terminal("p1", "failed")
    assert lifecycle.cancelled == []
    await gov.notify_task_terminal("p2", "failed")
    assert lifecycle.cancelled == ["c"]
    assert (await storage.get_task("c"))["status"] == "cancelled"


async def test_e2e_attached_keys_share_hot_record_ttl(storage):
    gov = _gov(storage)
    await storage.initialize_task("p1", expiry=100, initial_status="queued")
    await storage.initialize_task("c", expiry=100, initial_status="pending")
    await gov.register_dependency("c", ["p1"])

    for key in ("task:p1:active_children", "task:c:remaining_deps"):
        ttl = await storage.client.ttl(key)
        assert 0 < ttl <= 100, f"{key} ttl={ttl}"

    # update_task advances the hot record TTL; attached keys follow
    await storage.update_task("c", {"note": "x"}, expiry=400)
    ttl = await storage.client.ttl("task:c:remaining_deps")
    assert 100 < ttl <= 400


async def test_e2e_result_consumed_dedup_audit(storage):
    events = []

    class _Audit:
        async def append(self, event):
            events.append(event)

    gov = DependencyGovernor(
        storage, success_words=frozenset({"succeeded"}), audit_writer=_Audit()
    )
    await gov.result_consumed("c", "consumer-A")
    await gov.result_consumed("c", "consumer-A")
    assert len(events) == 1
    assert events[0].event_type == "result_consumed"