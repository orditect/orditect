
"""Orditect MVP demo: the full governance loop with zero infrastructure.

Stages:
  1 setup      - LocalFileStore (cold path) + in-memory hot-path doubles
  2 workflow   - recursive pipeline (the report node fails on first run)
  3 visualize  - tree / generations / dependency graph / audit / aggregate
  4 HITL retry - retry_scope through the action queue (reopen new generation)
  5 HITL pause - pause_node interrupts a slow node, resume_tree recovers
  6 validate   - run_rules checks the trace bundle (zero violations)

Run from the repository root:
    python examples/mvp/run_demo.py
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

import httpx  # noqa: E402

from orditect.adapter.local import LocalFileStore  # noqa: E402
from orditect.adapter.ui import ActionSinkAdapter, MemoryActionQueue  # noqa: E402
from orditect.bridge.openai import GovernedLLMClient  # noqa: E402
from orditect.flow import BudgetLedger, TaskOrchestrator  # noqa: E402
from orditect.flow.actions import ActionDispatcher  # noqa: E402
from orditect.flow.recovery import RecoveryService  # noqa: E402
from orditect.flow.snapshot import ProtocolSnapshotSink  # noqa: E402
from orditect.protocol import DependencyEdge  # noqa: E402
from orditect.protocol.rules import run_rules  # noqa: E402

from infra import InMemoryGovernor, InMemoryQuota, InMemoryTaskStorage  # noqa: E402
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
    """OpenAI-compatible bridge pointed at a mock endpoint (no API key needed).

    To use a real endpoint, drop the injected http_client and pass a real
    base_url / api_key, e.g. any OpenAI-compatible server (Ollama, vLLM,
    OpenAI). Nothing else changes.
    """

    async def mock_endpoint(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={
            "id": "chatcmpl-demo",
            "model": "gpt-4o-mock",
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": "mock-llm-answer"},
                "finish_reason": "stop",
            }],
            "usage": {"prompt_tokens": 12, "completion_tokens": 18, "total_tokens": 30},
        })

    http = httpx.AsyncClient(transport=httpx.MockTransport(mock_endpoint))
    return GovernedLLMClient(
        "http://mock",
        governor=governor,
        resource="llm",
        budget=budget,
        audit_writer=store.audit,
        content_writer=store.content,
        model="gpt-4o-mock",
        http_client=http,
    )


async def wait_receipt(sink: ActionSinkAdapter, action_id: str, timeout: float = 5.0):
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

    # ---- 1. setup -----------------------------------------------------
    store = LocalFileStore(TRACE_DIR)          # cold path (trace bundle)
    governor = InMemoryGovernor(capacity=2)    # hot-path double
    storage = InMemoryTaskStorage()            # hot-path double

    budget = BudgetLedger(InMemoryQuota(), root_task_id=ROOT_TASK_ID, max_units=100)
    await budget.open()

    orchestrator = TaskOrchestrator(
        storage,
        governor,
        # The master switch for visualization: lifecycle snapshots are
        # written into the protocol snapshot domain. Without this the
        # executor uses the NullSink and the trace bundle stays empty.
        snapshot_sink=ProtocolSnapshotSink(store.snapshot),
    )

    llm = make_llm(governor, budget, store)

    fail_flags: dict = {"report": True}
    recovery = RecoveryService(
        storage,
        store.snapshot,  # protocol SnapshotReader
        orchestrator.executor,
        # T6 vocabulary neutrality: the caller declares its success words.
        reuse_terminal_words=frozenset({"succeeded"}),
        task_factory=make_task_factory(storage, llm, fail_flags),
    )

    queue = MemoryActionQueue()
    sink = ActionSinkAdapter(queue, audit_writer=store.audit)
    dispatcher = ActionDispatcher(queue, orchestrator, recovery, poll_interval=0.2)
    await dispatcher.start()

    # Dependency edges (pure-edge facts, T12). They feed the dependency
    # graph view; in production these are written by DependencyGovernor.
    for child, parent in (
        ("collect", ROOT_TASK_ID),
        ("analyze", "collect"),
        ("report", "analyze"),
    ):
        await store.dependency.write_dependency(
            DependencyEdge(child_id=child, parent_id=parent, is_primary=True)
        )

    try:
        # ---- 2. workflow ----------------------------------------------
        print("==> run pipeline (collect -> analyze -> report[will fail])")
        await orchestrator.submit(
            PipelineTask(storage, orchestrator, llm, fail_flags),
            task_id=ROOT_TASK_ID,
        )
        root_record = await orchestrator.wait_terminal(ROOT_TASK_ID, timeout=30)
        print(f"pipeline finished: {root_record['status']}, "
              f"result={root_record.get('result')}")

        # ---- 3. visualize ----------------------------------------------
        await print_workflow_tree(TRACE_DIR, ROOT_TASK_ID)
        await print_dependencies(TRACE_DIR, ROOT_TASK_ID)
        await print_audit(TRACE_DIR)
        await print_stats(TRACE_DIR)
        print(f"\nbudget balance after first run: {await budget.balance()} / 100")

        # ---- 4. HITL retry ----------------------------------------------
        print("\n==> HITL: retry_scope('report') via the action queue")
        receipt = await sink.retry_scope(ROOT_TASK_ID, {"report"}, actor="demo-user")
        print(f"action accepted: {receipt.action_id}")
        print(f"action executed: {await wait_receipt(sink, receipt.action_id)}")
        report_record = await orchestrator.wait_terminal("report", timeout=30)
        print(f"report after retry: {report_record['status']} "
              f"(eid={report_record['execution_id']})")

        await print_workflow_tree(TRACE_DIR, ROOT_TASK_ID)
        await print_generations(TRACE_DIR, ROOT_TASK_ID)

        # ---- 5. HITL pause + resume --------------------------------------
        print("\n==> HITL: submit a slow node, pause it, then resume the tree")
        await orchestrator.submit(
            SlowTask(storage, steps=5, step_delay=0.6),
            task_id="slow",
            parent_task_id=ROOT_TASK_ID,
        )
        await asyncio.sleep(0.5)  # let it enter its working loop

        receipt = await sink.pause_node("slow", actor="demo-user")
        print(f"pause executed: {await wait_receipt(sink, receipt.action_id)}")
        slow_record = await orchestrator.wait_terminal("slow", timeout=15)
        print(f"slow after pause: {slow_record['status']}")

        receipt = await sink.resume_tree(ROOT_TASK_ID, actor="demo-user")
        print(f"resume executed: {await wait_receipt(sink, receipt.action_id)}")
        slow_record = await orchestrator.wait_terminal("slow", timeout=15)
        print(f"slow after resume: {slow_record['status']} "
              f"(eid={slow_record['execution_id']})")

        # Let the executor's shielded finalization (the first generation's
        # cancelled settle write) drain before printing/validating. The
        # orchestrator's bg tasks and the executor's finalize tasks both
        # complete here; afterwards no late snapshot write can race the
        # views below.
        await orchestrator.wait_all_finalized()

        await print_workflow_tree(TRACE_DIR, ROOT_TASK_ID)
        await print_generations(TRACE_DIR, ROOT_TASK_ID)
        await print_audit(TRACE_DIR)
        print(f"\nfinal budget balance: {await budget.balance()} / 100")

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


if __name__ == "__main__":
    asyncio.run(main())