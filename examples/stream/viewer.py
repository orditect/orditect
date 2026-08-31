"""Visualization helpers built on the adapter-ui consumer read surface.

Every view reads from the trace bundle (LocalFileStore output) via
TraceBundleReader — never from orditect-core / orditect-flow internals.
The reader loads the bundle at construction time, so each function builds
a fresh reader to see the latest data (cheap for this scale).
"""

from __future__ import annotations

from pathlib import Path

from orditect.adapter.ui import TraceBundleReader


def _reader(trace_dir: Path) -> TraceBundleReader:
    return TraceBundleReader(trace_dir)


async def print_workflow_tree(trace_dir: Path, root_id: str) -> None:
    """Lineage tree view: latest generation per node, nested by parent."""
    reader = _reader(trace_dir)
    latest = await reader.snapshot.get_tree(root_id, latest_only=True)
    by_parent: dict[str | None, list] = {}
    for snap in latest:
        by_parent.setdefault(snap.parent_task_id, []).append(snap)

    print("\n=== workflow tree (latest generation per node) ===")

    def walk(task_id: str, depth: int) -> None:
        children = sorted(by_parent.get(task_id, []), key=lambda s: s.task_id)
        for snap in children:
            print(f"{'  ' * depth}+- {snap.task_id:<16} [{snap.status}]")
            walk(snap.task_id, depth + 1)

    roots = sorted(by_parent.get(None, []), key=lambda s: s.task_id)
    for snap in roots:
        print(f"{snap.task_id:<18} [{snap.status}]")
        walk(snap.task_id, 1)
    if not by_parent:
        print("  <empty>")


async def print_generations(trace_dir: Path, root_id: str) -> None:
    """Time-travel view: every execution generation of every node."""
    reader = _reader(trace_dir)
    full = await reader.snapshot.get_tree(root_id, latest_only=False)
    generations: dict[str, list[str]] = {}
    for snap in full:
        generations.setdefault(snap.task_id, []).append(
            f"{snap.execution_id}:{snap.status}"
        )
    print("\n=== execution generations (time-travel view) ===")
    for task_id in sorted(generations):
        print(f"  {task_id:<16} {'  ->  '.join(generations[task_id])}")


async def print_dependencies(trace_dir: Path, root_id: str) -> None:
    """Dependency graph view (pure-edge facts, T12)."""
    reader = _reader(trace_dir)
    graph = await reader.dependency.read_graph(root_id)
    print("\n=== dependency graph (pure-edge facts) ===")
    print(f"  nodes: {graph.task_ids}")
    for edge in graph.edges:
        mark = " (primary)" if edge.is_primary else ""
        print(f"  {edge.child_id} depends on {edge.parent_id}{mark}")


async def print_audit(trace_dir: Path) -> None:
    """Audit log view (append-only, idempotent event log)."""
    reader = _reader(trace_dir)
    rows = await reader.audit.query()
    print("\n=== audit events (append-only log) ===")
    for event in rows:
        usage = event.payload.get("usage") or {}
        tokens = usage.get("total_tokens")
        elapsed = event.payload.get("elapsed_ms")
        extra = f"tokens={tokens} elapsed={elapsed}ms" if tokens else ""
        print(f"  [{event.event_type:<14}] id={event.event_id} {extra}")


async def print_stats(trace_dir: Path) -> None:
    """Aggregate view: node counts grouped by status."""
    reader = _reader(trace_dir)
    stats = await reader.snapshot.aggregate(group_by="status")
    print("\n=== aggregate by status ===")
    for status, bucket in sorted(stats.items()):
        print(f"  {status:<12} count={bucket['count']}")