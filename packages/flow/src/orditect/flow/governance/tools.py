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
    for edge in edges:
        children.setdefault(edge.child_id, []).append(edge.parent_id)
        nodes.add(edge.child_id)
        nodes.add(edge.parent_id)

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


async def rebuild_dep_counters(
    storage: Any,
    dep_graph_store: Any,
    *,
    success_words: frozenset[str] = frozenset({"succeeded"}),
    terminal_words: frozenset[str] = frozenset(
        {"succeeded", "failed", "cancelled"}
    ),
    lifecycle: Any = None,
) -> dict[str, Any]:
    """Rebuild remaining_deps counters from the cold store (admin recovery).

    Intended use: Redis restarted and lost counters/sets. v0.1.5 hardening:
    - a child with ANY missing parent hot record is skipped AS A WHOLE
      (was: silently under-counted -> premature readiness);
    - threshold-reaching votes trigger lifecycle.cancel when injected, and
      are always reported (was: silently dangling);
    - status vocabulary is caller-declared (was: hardcoded, violating T6).

    Returns:
        {"rebuilt": int, "skipped": int, "skipped_children": list[str],
         "cancelled": list[str], "pending_cancel": list[str], "errors": int}
    """
    edges = await dep_graph_store.all_edges()
    parents_of: dict[str, list[str]] = {}
    for edge in edges:
        parents_of.setdefault(edge.child_id, []).append(edge.parent_id)

    stats: dict[str, Any] = {
        "rebuilt": 0,
        "skipped": 0,
        "skipped_children": [],
        "cancelled": [],
        "pending_cancel": [],
        "errors": 0,
    }
    for child_id, parents in parents_of.items():
        try:
            child_rec = await storage.get_task(child_id)
            if not child_rec:
                stats["skipped"] += 1
                continue

            running = 0
            reached_cancel = False
            missing_parent = False
            for parent_id in parents:
                parent_rec = await storage.get_task(parent_id)
                if not parent_rec:
                    # A missing parent hot record means the cold record alone
                    # cannot prove the parent's status — under-counting would
                    # risk premature readiness, so skip the whole child.
                    missing_parent = True
                    break
                status = parent_rec.get("status", "")
                if status in terminal_words:
                    if status not in success_words:
                        if await storage.vote_and_check_threshold(
                            child_id, parent_id, len(parents)
                        ):
                            reached_cancel = True
                else:
                    running += 1
                    await storage.sadd_active_child(parent_id, child_id)

            if missing_parent:
                stats["skipped_children"].append(child_id)
                continue

            await storage.set_remaining_deps(child_id, running)
            stats["rebuilt"] += 1

            if reached_cancel:
                if lifecycle is not None:
                    await lifecycle.cancel(child_id)
                    stats["cancelled"].append(child_id)
                else:
                    stats["pending_cancel"].append(child_id)
        except Exception as e:
            stats["errors"] += 1
            logger.error(f"counter rebuild failed: {child_id}, {e}")
    return stats