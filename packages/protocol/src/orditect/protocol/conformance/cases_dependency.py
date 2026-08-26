"""Dependency-domain conformance cases (CF-DEP-*).

Authoring discipline: one adapter instance runs the WHOLE suite in a single
event loop — every case MUST use case-unique task_id prefixes, never shared
placeholders.
"""

from __future__ import annotations

from typing import Any, Awaitable, Callable

from orditect.protocol.errors import IdempotencyConflictError
from orditect.protocol.models import DependencyEdge

CaseFn = Callable[[Any], Awaitable[None]]
CASES: list[tuple[str, str, CaseFn]] = []


def case(case_id: str):
    def deco(fn: CaseFn) -> CaseFn:
        CASES.append((case_id, "dependency_sink", fn))
        return fn
    return deco


def _edge(child: str, parent: str, primary: bool = False) -> DependencyEdge:
    return DependencyEdge(child_id=child, parent_id=parent, is_primary=primary)


@case("CF-DEP-001")
async def write_read_roundtrip(adapter: Any) -> None:
    """CF-DEP-001 (T12): a written edge reads back with full fidelity."""
    if not adapter.capabilities.supports("dependency_query"):
        return
    edge = _edge("cf-dep-001-c", "cf-dep-001-p", primary=True)
    await adapter.write_dependency(edge)
    edges = await adapter.all_edges()
    assert any(
        e.child_id == edge.child_id and e.parent_id == edge.parent_id
        and e.is_primary is True
        for e in edges
    ), "written edge not found or fields not preserved"


@case("CF-DEP-002")
async def rewrite_same_edge_idempotent(adapter: Any) -> None:
    """CF-DEP-002 (T4): re-writing the same (child, parent) key dedups."""
    if not adapter.capabilities.supports("dependency_query"):
        return
    edge = _edge("cf-dep-002-c", "cf-dep-002-p", primary=True)
    await adapter.write_dependency(edge)
    await adapter.write_dependency(edge)  # identical -> silent success
    edges = [
        e for e in await adapter.all_edges()
        if e.child_id == "cf-dep-002-c" and e.parent_id == "cf-dep-002-p"
    ]
    assert len(edges) == 1, f"expected exactly 1 edge, got {len(edges)}"


@case("CF-DEP-003")
async def same_key_different_primary_conflicts(adapter: Any) -> None:
    """CF-DEP-003 (T4): same key with different is_primary raises conflict."""
    await adapter.write_dependency(_edge("cf-dep-003-c", "cf-dep-003-p", primary=True))
    try:
        await adapter.write_dependency(
            _edge("cf-dep-003-c", "cf-dep-003-p", primary=False)
        )
    except IdempotencyConflictError:
        return
    raise AssertionError("expected IdempotencyConflictError, got success")


@case("CF-DEP-004")
async def read_graph_closure_cycle_safe(adapter: Any) -> None:
    """CF-DEP-004 (T12): bidirectional transitive closure; cycles terminate."""
    if not adapter.capabilities.supports("dependency_query"):
        return
    # chain: c -> b -> a, plus branch c -> d; plus mutual cycle x <-> y
    await adapter.write_dependency(_edge("cf-dep-004-c", "cf-dep-004-b"))
    await adapter.write_dependency(_edge("cf-dep-004-b", "cf-dep-004-a"))
    await adapter.write_dependency(_edge("cf-dep-004-c", "cf-dep-004-d"))
    await adapter.write_dependency(_edge("cf-dep-004-x", "cf-dep-004-y"))
    await adapter.write_dependency(_edge("cf-dep-004-y", "cf-dep-004-x"))

    graph = await adapter.read_graph("cf-dep-004-a")
    ids = set(graph.task_ids)
    # downstream from a: b, c, d all reachable; upstream: a itself
    assert {"cf-dep-004-a", "cf-dep-004-b", "cf-dep-004-c", "cf-dep-004-d"} <= ids

    cycle_graph = await adapter.read_graph("cf-dep-004-x")  # must terminate
    assert {"cf-dep-004-x", "cf-dep-004-y"} <= set(cycle_graph.task_ids)

    rootless = await adapter.read_graph("cf-dep-004-lonely")
    assert rootless.task_ids == ["cf-dep-004-lonely"]


@case("CF-DEP-005")
async def neighbor_direction_correct(adapter: Any) -> None:
    """CF-DEP-005 (T12): children_of / parents_of are direction-true, one level."""
    if not adapter.capabilities.supports("dependency_query"):
        return
    await adapter.write_dependency(_edge("cf-dep-005-c1", "cf-dep-005-p"))
    await adapter.write_dependency(_edge("cf-dep-005-c2", "cf-dep-005-p"))
    await adapter.write_dependency(_edge("cf-dep-005-p", "cf-dep-005-gp"))

    assert set(await adapter.children_of("cf-dep-005-p")) == {
        "cf-dep-005-c1", "cf-dep-005-c2",
    }
    assert await adapter.parents_of("cf-dep-005-p") == ["cf-dep-005-gp"]
    # one level only: grandchildren of gp do not appear in children_of(gp)
    assert set(await adapter.children_of("cf-dep-005-gp")) == {"cf-dep-005-p"}


@case("CF-DEP-006")
async def is_primary_flag_preserved(adapter: Any) -> None:
    """CF-DEP-006 (T12): the is_primary flag survives the write/read chain."""
    if not adapter.capabilities.supports("dependency_query"):
        return
    await adapter.write_dependency(_edge("cf-dep-006-c", "cf-dep-006-p1", primary=True))
    await adapter.write_dependency(_edge("cf-dep-006-c", "cf-dep-006-p2", primary=False))
    edges = {
        e.parent_id: e.is_primary
        for e in await adapter.all_edges()
        if e.child_id == "cf-dep-006-c"
    }
    assert edges.get("cf-dep-006-p1") is True
    assert edges.get("cf-dep-006-p2") is False