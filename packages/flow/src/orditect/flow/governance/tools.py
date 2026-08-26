"""Offline dependency-governance tools (v0.1.1).

Both tools operate on the injected cold dep_graph_store — never on the
hot path. Deployment form: manual ops / cron jobs, not runtime machinery.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


async def scan_dependency_cycles(dep_graph_store: Any) -> list[list[str]]:
    """Full-graph DFS over the cold store (cycle-detection line 2 of 2).

    The register-time DFS (line 1) admits a small miss window under
    concurrent registrations; this offline scan closes it. Returns every
    detected cycle as a node list (alarm on non-empty).
    """
    edges = await dep_graph_store.all_edges()
    children: dict[str, list[str]] = {}
    nodes: set[str] = set()
    for child_id, parent_id in edges:
        children.setdefault(child_id, []).append(parent_id)
        nodes.add(child_id)
        nodes.add(parent_id)

    cycles: list[list[str]] = []
    seen_cycles: set[tuple[str, ...]] = set()
    visited: set[str] = set()  # proven acyclic (memoized)
    in_stack: set[str] = set()

    def walk(node: str, path: list[str]) -> None:
        if node in in_stack:
            cycle = path[path.index(node):] + [node]
            canonical = tuple(sorted(set(cycle)))
            if canonical not in seen_cycles:
                seen_cycles.add(canonical)
                cycles.append(cycle)
            return
        if node in visited:
            return
        in_stack.add(node)
        for parent in children.get(node, []):
            walk(parent, path + [parent])
        in_stack.discard(node)
        visited.add(node)

    for node in sorted(nodes):
        walk(node, [node])
    return cycles


async def rebuild_dep_counters(storage: Any, dep_graph_store: Any) -> dict[str, int]:
    """Rebuild remaining_deps counters from the cold store (admin recovery).

    Intended use: Redis restarted and lost counters/sets. Formula per
    child: running_parents = parents - (terminal parents). Terminal
    parents that are not success re-add their cancel vote (failure votes
    are re-derivable from cold state); vote sets for children whose
    votes came from live vote_cancel calls may be incomplete afterward —
    the admin path is best-effort by definition.

    Returns:
        {"rebuilt": int, "skipped": int, "errors": int}
    """
    edges = await dep_graph_store.all_edges()
    parents_of: dict[str, list[str]] = {}
    for child_id, parent_id in edges:
        parents_of.setdefault(child_id, []).append(parent_id)

    stats = {"rebuilt": 0, "skipped": 0, "errors": 0}
    for child_id, parents in parents_of.items():
        try:
            child_rec = await storage.get_task(child_id)
            if not child_rec:
                stats["skipped"] += 1
                continue

            running = 0
            for parent_id in parents:
                parent_rec = await storage.get_task(parent_id)
                if not parent_rec:
                    # parent hot record lost: conservative skip — cold
                    # records alone cannot prove the parent's status
                    stats["skipped"] += 1
                    continue
                status = parent_rec.get("status", "")
                if status in ("succeeded", "failed", "cancelled"):
                    if status != "succeeded":
                        await storage.vote_and_check_threshold(
                            child_id, parent_id, len(parents)
                        )
                else:
                    running += 1
                    await storage.sadd_active_child(parent_id, child_id)

            await storage.set_remaining_deps(child_id, running)
            stats["rebuilt"] += 1
        except Exception as e:
            stats["errors"] += 1
            logger.error(f"counter rebuild failed: {child_id}, {e}")
    return stats