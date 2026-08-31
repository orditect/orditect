"""Orditect dependency-governance demo: multi-parent fan-in (zero infra).

Scenario: C runs only after A AND B finish — the case a linear pipeline
(and recursive composition) cannot express. This example drives the full
DependencyGovernor lifecycle from the caller side, because the governor
is PASSIVE: it never creates tasks and never schedules execution.

Stages:
  1 setup      - LocalFileStore (cold path) + in-memory hot-path doubles
  2 register   - register_dependency for two fan-in children
  3 notify     - parents finish; success decrements, failure auto-votes
  4 readiness  - get_ready_tasks answers "what may I start now"
  5 voting     - all parents failed -> child cancelled via lifecycle
  6 observe    - dependency graph vs snapshot tree (two distinct views)
  7 validate   - run_rules over the produced trace bundle

Run from the repository root:
    python examples/dependency-governance/run_demo.py
"""

from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import bootstrap  # noqa: F401,E402  (must precede every orditect import)

from orditect.adapter.local import LocalFileStore  # noqa: E402
from orditect.adapter.ui import TraceBundleReader  # noqa: E402
from orditect.flow import BudgetLedger, TaskOrchestrator  # noqa: E402
from orditect.flow.governance import DependencyGovernor  # noqa: E402
from orditect.flow.snapshot import ProtocolSnapshotSink  # noqa: E402
from orditect.protocol.rules import run_rules  # noqa: E402

from infra import InMemoryGovernor, InMemoryQuota, InMemoryTaskStorage  # noqa: E402
from tasks import FanInTask, LeafTask, make_task_factory  # noqa: E402

DEMO_DIR = Path(__file__).resolve().parent / "demo_data"
TRACE_DIR = DEMO_DIR / "trace"


async def submit_and_wait(
    orchestrator: TaskOrchestrator,
    storage,
    gov: DependencyGovernor,
    task,
    task_id: str,
    should_fail: bool = False,
) -> dict:
    """Submit one node, wait for its terminal state, then NOTIFY the governor.

    The notification is the caller's contract (Ch.8.5): the built-in
    executor never calls notify_task_terminal — wiring it at task-closure
    points is your responsibility. This helper is exactly that wiring.
    """
    await orchestrator.submit(task, task_id=task_id)
    record = await orchestrator.wait_terminal(task_id, timeout=30)
    await gov.notify_task_terminal(task_id, record["status"])
    return record


async def main() -> None:
    shutil.rmtree(DEMO_DIR, ignore_errors=True)

    # ---- 1. setup -----------------------------------------------------
    store = LocalFileStore(TRACE_DIR)          # cold path (trace bundle)
    governor = InMemoryGovernor(capacity=4)    # hot-path double
    storage = InMemoryTaskStorage()            # hot-path double

    budget = BudgetLedger(InMemoryQuota(), root_task_id="dep-demo", max_units=1000)
    await budget.open()

    orchestrator = TaskOrchestrator(
        storage,
        governor,
        snapshot_sink=ProtocolSnapshotSink(store.snapshot),
    )

    gov = DependencyGovernor(
        storage,
        success_words=frozenset({"succeeded"}),     # REQUIRED (T6)
        lifecycle=orchestrator.lifecycle,           # vote-triggered cancel
        audit_writer=store.audit,                   # optional
        dep_graph_store=store.dependency,           # cold path: graph query works
    )

    # ---- 2. register: C1 depends on A AND B (C2 is added later) -------
    print("==> register fan-in: c1 depends on [a, b]")
    # Task creation is YOUR job; registration records the relationship.
    await storage.initialize_task("a", "pending")
    await storage.initialize_task("b", "pending")
    await storage.initialize_task("c1", "pending")
    await gov.register_dependency("c1", ["a", "b"], primary_parent="a")
    print(f"ready tasks after registration: {await gov.get_ready_tasks()}  "
          f"(none: parents not finished)")

    # ---- 3+4. parents succeed; readiness surfaces ----------------------
    print("\n==> parents succeed one by one (caller notifies each terminal)")
    r = await submit_and_wait(orchestrator, storage, gov,
                              LeafTask(storage, work=0.1), "a")
    print(f"a finished: {r['status']}; ready: {await gov.get_ready_tasks()}")

    r = await submit_and_wait(orchestrator, storage, gov,
                              LeafTask(storage, work=0.1), "b")
    print(f"b finished: {r['status']}; ready: {await gov.get_ready_tasks()}")

    # ---- the caller decides to start C1 (the governor schedules nothing)
    print("\n==> c1 is ready; the CALLER submits it")
    r = await submit_and_wait(
        orchestrator, storage, gov,
        FanInTask(storage, needs=["a", "b"]), "c1",
    )
    print(f"c1 finished: {r['status']}, result={r.get('result')}")
    assert r["status"] == "succeeded"

    # ---- 5. voting: all parents failed -> child cancelled --------------
    print("\n==> voting discipline: c2 depends on [x, y]; x fails, y fails")
    await storage.initialize_task("x", "pending")
    await storage.initialize_task("y", "pending")
    await storage.initialize_task("c2", "pending")
    await gov.register_dependency("c2", ["x", "y"], primary_parent="x")

    r = await submit_and_wait(orchestrator, storage, gov,
                              LeafTask(storage, fail=True), "x")
    print(f"x finished: {r['status']} (auto-vote cast on c2)")
    c2 = await storage.get_task("c2")
    print(f"c2 status after x failed: {c2['status']} (still pending: 1/2 votes)")

    r = await submit_and_wait(orchestrator, storage, gov,
                              LeafTask(storage, fail=True), "y")
    print(f"y finished: {r['status']} (auto-vote: threshold reached)")
    c2 = await storage.get_task("c2")
    print(f"c2 status after y failed: {c2['status']} "
          f"(cancelled via lifecycle — hang prevention)")
    assert c2["status"] == "cancelled"

    # ---- 6. observe: two views that must not be conflated --------------
    reader = TraceBundleReader(TRACE_DIR)
    # read_graph is a transitive CLOSURE from one root; our two fan-ins are
    # disconnected components, so no single root shows both. all_edges()
    # enumerates every recorded edge (offline scan surface, Ch.7.1).
    print("\n=== dependency graph (structure: who depends on whom) ===")
    all_edges = await reader.dependency.all_edges()
    for e in sorted(all_edges, key=lambda e: (e.child_id, e.parent_id)):
        mark = " (primary)" if e.is_primary else ""
        print(f"  {e.child_id} depends on {e.parent_id}{mark}")
    graph = await reader.dependency.read_graph("c2")
    print(f"  (closure from c2 reaches only its own component: "
          f"{graph.task_ids} — read_graph answers 'what is reachable from "
          f"here', not 'list everything'.)")

    # The snapshot tree walks parent_task_id lineage. These nodes were all
    # submitted top-level (parent_task_id=None), so each is its own root —
    # the tree view answers "where is the run", not "who depends on whom".
    print("\n=== snapshot tree (execution state: where the run is) ===")
    for root in ("a", "x"):
        tree = await reader.snapshot.get_tree(root, latest_only=True)
        for s in sorted(tree, key=lambda s: s.task_id):
            print(f"  {s.task_id:<6} [{s.status}]")
    print("  (c2 has no snapshot row: it was cancelled by voting BEFORE ever "
          "running — no execution generation exists for it. That is why "
          "run_rules reports exactly one DR-DEP-001 warning below.)")

    # ---- 7. validate ----------------------------------------------------
    lines: list[dict] = []
    for name in ("snapshots.ndjson", "audit.ndjson", "deps.ndjson"):
        path = TRACE_DIR / name
        if path.is_file():
            lines.extend(json.loads(x) for x in path.read_text().splitlines())
    report = run_rules(lines)
    print(f"\n==> data rules: {report.summary()}")
    assert report.ok
    print("\nDEMO OK - trace bundle at:", TRACE_DIR)


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())