"""Consumer-profile seeded read cases (CF-VIEW-*).

Scope discipline: CF-VIEW cases verify deep read semantics over PRE-SEEDED
data (trees, expiry, graph closures) — they do NOT repeat the write/read
round-trip assertions of the sink cases.

Authoring discipline: one adapter instance runs the whole suite; all ids
carry the "cv-" prefix (shared with fixtures.py), never shared placeholders.
"""

from __future__ import annotations

from typing import Any, Awaitable, Callable

CaseFn = Callable[[Any], Awaitable[None]]
CASES: list[tuple[str, str, CaseFn]] = []


def case(case_id: str):
    def deco(fn: CaseFn) -> CaseFn:
        CASES.append((case_id, "view", fn))
        return fn
    return deco


@case("CF-VIEW-001")
async def tree_latest_vs_all_generations(adapter: Any) -> None:
    """CF-VIEW-001 (T11): get_tree latest_only collapses generations; False keeps all."""
    if not adapter.capabilities.supports("snapshot_query"):
        return
    latest = await adapter.get_tree("cv-root", latest_only=True)
    ids_latest = {(s.task_id, s.execution_id) for s in latest}
    # cv-a appears exactly once, at its latest generation e2
    assert ("cv-a", "e2") in ids_latest
    assert ("cv-a", "e1") not in ids_latest
    assert ("cv-root", "e1") in ids_latest

    full = await adapter.get_tree("cv-root", latest_only=False)
    ids_full = {(s.task_id, s.execution_id) for s in full}
    assert ("cv-a", "e1") in ids_full
    assert ("cv-a", "e2") in ids_full


@case("CF-VIEW-002")
async def expired_invisible_to_readers(adapter: Any) -> None:
    """CF-VIEW-002 (T1): expired snapshots are invisible to get and query."""
    if not adapter.capabilities.supports("snapshot_query"):
        return
    got = await adapter.get("cv-expired", "execute")
    assert got is None, "expired snapshot must be invisible to get (T1)"
    rows = await adapter.query()
    assert all(s.task_id != "cv-expired" for s in rows), (
        "expired snapshot must be invisible to query (T1)"
    )


@case("CF-VIEW-003")
async def graph_closure_and_neighbors(adapter: Any) -> None:
    """CF-VIEW-003 (T12): read_graph closure + children_of/parents_of direction."""
    if not adapter.capabilities.supports("dependency_query"):
        return
    graph = await adapter.read_graph("cv-root")
    assert {"cv-root", "cv-a", "cv-b", "cv-a1"} <= set(graph.task_ids)

    children = set(await adapter.children_of("cv-root"))
    assert {"cv-a", "cv-b"} <= children
    parents = set(await adapter.parents_of("cv-a"))
    assert "cv-root" in parents

@case("CF-VIEW-004")
async def status_filter_and_aggregate(adapter: Any) -> None:
    """CF-VIEW-004 (T6): query(status=...) filters; aggregate buckets count
    over latest generations only."""
    if not adapter.capabilities.supports("snapshot_query"):
        return
    rows = await adapter.query(status="running")
    assert rows, "expected at least one running snapshot"
    assert all(s.status == "running" for s in rows)
    assert all(s.task_id != "cv-expired" for s in rows)  # T1 still applies

    # aggregate folds to latest generations: cv-a counts as running (e2),
    # so done = {cv-root, cv-a1} = 2 and running includes cv-a.
    out = await adapter.aggregate(group_by="status")
    assert out.get("done", {}).get("count", 0) == 2, (
        "done bucket must hold latest-generation done nodes only "
        "(cv-root, cv-a1); cv-a's latest generation is running"
    )
    assert out.get("failed", {}).get("count", 0) == 1  # cv-b
    assert out.get("running", {}).get("count", 0) == 1  # cv-a (e2, latest)