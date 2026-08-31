"""Orditect real-world demo: production hot path (Redis) + real LLM.

Identical business code to examples/mvp — only the hot-path doubles and
the LLM endpoint are swapped for their production counterparts. This is
the living proof of Orditect's hot/cold replaceability.

Prereqs:
  1. Redis reachable at REDIS_URL.
  2. An OpenAI-compatible endpoint reachable at LLM_BASE_URL with LLM_MODEL
     available (e.g. `ollama serve` + `ollama pull qwen2.5:7b`).
  3. cp .env.example .env  (and edit values).

Run from the repository root:
    python examples/real-world/run_demo.py
"""

from __future__ import annotations

import asyncio
import json
import shutil
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import bootstrap  # noqa: F401,E402  (must precede every orditect import)

from orditect.adapter.local import LocalFileStore  # noqa: E402
from orditect.adapter.ui import ActionSinkAdapter, MemoryActionQueue  # noqa: E402
from orditect.bridge.openai import GovernedLLMClient  # noqa: E402
from orditect.flow import BudgetLedger, TaskOrchestrator  # noqa: E402
from orditect.flow.actions import ActionDispatcher  # noqa: E402
from orditect.flow.recovery import RecoveryService  # noqa: E402
from orditect.flow.snapshot import ProtocolSnapshotSink  # noqa: E402
from orditect.protocol import DependencyEdge  # noqa: E402
from orditect.protocol.rules import run_rules  # noqa: E402

from infra import build_hot_path  # noqa: E402
from settings import SETTINGS  # noqa: E402
from tasks import ROOT_TASK_ID, PipelineTask, SlowTask, make_task_factory  # noqa: E402
from viewer import (  # noqa: E402
    print_audit,
    print_dependencies,
    print_generations,
    print_stats,
    print_workflow_tree,
)

DEMO_DIR = Path(__file__).resolve().parent / "demo_data"
TRACE_DIR = DEMO_DIR / "trace"


def make_llm(governor, budget, store) -> GovernedLLMClient:
    """OpenAI-compatible bridge pointed at the configured real endpoint."""
    return GovernedLLMClient(
        SETTINGS.llm_base_url,
        api_key=SETTINGS.llm_api_key,
        governor=governor,
        resource="llm",
        budget=budget,
        audit_writer=store.audit,
        content_writer=store.content,
        model=SETTINGS.llm_model,
        timeout=120.0,
    )


async def wait_receipt(sink: ActionSinkAdapter, action_id: str, timeout: float = 10.0):
    """Poll the action queue for the execution receipt of one action."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        receipt = await sink.get_receipt(action_id)
        if receipt is not None:
            return receipt
        await asyncio.sleep(0.05)
    raise TimeoutError(f"no receipt for action {action_id}")


async def main() -> None:
    shutil.rmtree(DEMO_DIR, ignore_errors=True)

    # ---- 1. setup: real Redis hot path + local trace bundle (cold) ----
    store = LocalFileStore(TRACE_DIR)
    storage, governor, quota, redis_client = await build_hot_path()

    budget = BudgetLedger(
        quota, root_task_id=ROOT_TASK_ID, max_units=SETTINGS.budget_max_units
    )
    await budget.open()

    orchestrator = TaskOrchestrator(
        storage,
        governor,
        snapshot_sink=ProtocolSnapshotSink(store.snapshot),
    )

    llm = make_llm(governor, budget, store)

    fail_flags: dict = {"report": True}
    recovery = RecoveryService(
        storage,
        store.snapshot,
        orchestrator.executor,
        reuse_terminal_words=frozenset({"succeeded"}),
        task_factory=make_task_factory(storage, llm, fail_flags, orchestrator),
    )

    queue = MemoryActionQueue()
    sink = ActionSinkAdapter(queue, audit_writer=store.audit)
    dispatcher = ActionDispatcher(queue, orchestrator, recovery, poll_interval=0.2)
    await dispatcher.start()

    for child, parent in (
        ("collect", ROOT_TASK_ID),
        ("analyze", "collect"),
        ("report", "analyze"),
    ):
        await store.dependency.write_dependency(
            DependencyEdge(child_id=child, parent_id=parent, is_primary=True)
        )

    try:
        # ---- 2. workflow (identical business code to the MVP) -----------
        print("==> run pipeline (collect -> analyze -> report[will fail])")
        await orchestrator.submit(
            PipelineTask(storage, orchestrator, llm, fail_flags),
            task_id=ROOT_TASK_ID,
        )
        root_record = await orchestrator.wait_terminal(ROOT_TASK_ID, timeout=300)
        print(f"pipeline finished: {root_record['status']}, "
              f"result={root_record.get('result')}")

        # ---- 3. visualize ----------------------------------------------
        await print_workflow_tree(TRACE_DIR, ROOT_TASK_ID)
        await print_dependencies(TRACE_DIR, ROOT_TASK_ID)
        await print_audit(TRACE_DIR)
        await print_stats(TRACE_DIR)
        print(f"\nbudget balance after first run: "
              f"{await budget.balance()} / {SETTINGS.budget_max_units}")

        # ---- 4. HITL retry ----------------------------------------------
        print("\n==> HITL: retry_scope('report') via the action queue")
        receipt = await sink.retry_scope(ROOT_TASK_ID, {"report"}, actor="demo-user")
        print(f"action accepted: {receipt.action_id}")
        print(f"action executed: {await wait_receipt(sink, receipt.action_id)}")
        report_record = await orchestrator.wait_terminal("report", timeout=300)
        print(f"report after retry: {report_record['status']} "
              f"(eid={report_record['execution_id']})")

        await print_workflow_tree(TRACE_DIR, ROOT_TASK_ID)
        await print_generations(TRACE_DIR, ROOT_TASK_ID)

        # ---- 5. HITL pause + resume --------------------------------------
        print("\n==> HITL: submit a slow node, pause it, then resume the tree")
        await orchestrator.submit(
            SlowTask(storage, steps=5, step_delay=1.0),
            task_id="slow",
            parent_task_id=ROOT_TASK_ID,
        )
        await asyncio.sleep(0.5)

        receipt = await sink.pause_node("slow", actor="demo-user")
        print(f"pause executed: {await wait_receipt(sink, receipt.action_id)}")
        slow_record = await orchestrator.wait_terminal("slow", timeout=60)
        print(f"slow after pause: {slow_record['status']}")

        receipt = await sink.resume_tree(ROOT_TASK_ID, actor="demo-user")
        print(f"resume executed: {await wait_receipt(sink, receipt.action_id)}")
        slow_record = await orchestrator.wait_terminal("slow", timeout=60)
        print(f"slow after resume: {slow_record['status']} "
              f"(eid={slow_record['execution_id']})")

        await print_workflow_tree(TRACE_DIR, ROOT_TASK_ID)
        await print_generations(TRACE_DIR, ROOT_TASK_ID)
        await print_audit(TRACE_DIR)
        print(f"\nfinal budget balance: "
              f"{await budget.balance()} / {SETTINGS.budget_max_units}")

        # ---- 6. validate --------------------------------------------------
        lines: list[dict] = []
        for name in ("snapshots.ndjson", "audit.ndjson", "deps.ndjson"):
            path = TRACE_DIR / name
            if path.is_file():
                lines.extend(json.loads(x) for x in path.read_text().splitlines())
        report = run_rules(lines)
        print(f"\n==> data rules: {report.summary()}")
        assert report.ok
        print("\nDEMO OK - trace bundle at:", TRACE_DIR)
    finally:
        await dispatcher.stop()
        await orchestrator.wait_all_finalized()  # drain bg + finalize tasks
        await redis_client.aclose()


if __name__ == "__main__":
    asyncio.run(main())