"""Orditect stream demo: governed workflow with a live SSE output plane.

Same Redis hot path and real LLM as examples/real-world, but the analyze
node STREAMS its LLM output to end users via the orditect-stream protocol
(stream.delta / enrich.* / stage.end / stream.manifest), while remaining
a flow-governed node. This is the governance-kernel + output-plane combo.

Prereqs: same as examples/real-world (Redis + OpenAI-compatible endpoint,
cp .env.example .env).

Run from the repository root:
    python examples/stream/run_demo.py
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
from sse_print import make_printer  # noqa: E402
from tasks import ROOT_TASK_ID, PipelineTask, make_task_factory  # noqa: E402

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

    store = LocalFileStore(TRACE_DIR)
    storage, governor, quota, redis_client = await build_hot_path()
    budget = BudgetLedger(
        quota, root_task_id=ROOT_TASK_ID, max_units=SETTINGS.budget_max_units
    )
    await budget.open()
    orchestrator = TaskOrchestrator(
        storage, governor, snapshot_sink=ProtocolSnapshotSink(store.snapshot)
    )
    llm = make_llm(governor, budget, store)
    on_event = make_printer(verbose_delta=True)
    fail_flags: dict = {"report": True}
    recovery = RecoveryService(
        storage, store.snapshot, orchestrator.executor,
        reuse_terminal_words=frozenset({"succeeded"}),
        task_factory=make_task_factory(storage, llm, store, fail_flags, orchestrator, on_event),
    )
    queue = MemoryActionQueue()
    sink = ActionSinkAdapter(queue, audit_writer=store.audit)
    dispatcher = ActionDispatcher(queue, orchestrator, recovery, poll_interval=0.2)
    await dispatcher.start()

    for child, parent in (
        ("collect", ROOT_TASK_ID), ("analyze", "collect"), ("report", "analyze"),
    ):
        await store.dependency.write_dependency(
            DependencyEdge(child_id=child, parent_id=parent, is_primary=True)
        )

    try:
        print("==> run pipeline; the analyze node streams SSE below (Ctrl+C to cancel)")
        await orchestrator.submit(
            PipelineTask(storage, orchestrator, llm, store, fail_flags, on_event),
            task_id=ROOT_TASK_ID,
        )
        root_record = await orchestrator.wait_terminal(ROOT_TASK_ID, timeout=300)
        print(f"\npipeline finished: {root_record['status']}, result={root_record.get('result')}")

        print("\n==> HITL: retry_scope('report') via the action queue")
        receipt = await sink.retry_scope(ROOT_TASK_ID, {"report"}, actor="demo-user")
        print(f"action executed: {await wait_receipt(sink, receipt.action_id)}")
        report_record = await orchestrator.wait_terminal("report", timeout=300)
        print(f"report after retry: {report_record['status']}")

        from viewer import print_audit, print_generations, print_workflow_tree
        await print_workflow_tree(TRACE_DIR, ROOT_TASK_ID)
        await print_generations(TRACE_DIR, ROOT_TASK_ID)
        await print_audit(TRACE_DIR)

        lines: list[dict] = []
        for name in ("snapshots.ndjson", "audit.ndjson", "deps.ndjson"):
            path = TRACE_DIR / name
            if path.is_file():
                lines.extend(json.loads(x) for x in path.read_text().splitlines())
        report = run_rules(lines)
        print(f"\n==> data rules: {report.summary()}")
        assert report.ok
        print("\nDEMO OK - trace bundle at:", TRACE_DIR)

    except KeyboardInterrupt:
        # Ctrl+C during the stream: cancel the tree gracefully, then fall
        # through to the cleanup in finally. The partial content is preserved
        # in the trace bundle (cancelled snapshot), nothing is lost.
        print("\n\n==> interrupted: cancelling the task tree")
        try:
            await orchestrator.cancel(ROOT_TASK_ID)
        except Exception as e:
            print(f"cancel best-effort failed (ignored): {e}")
        print("partial content is preserved in the trace bundle")
    finally:
        # Deterministic cleanup order: stop the dispatcher, drain executor
        # finalization, then close external connections. This ordering is
        # what prevents 'Event loop is closed' noise on Ctrl+C.
        try:
            await dispatcher.stop()
        except Exception:
            pass
        try:
            await orchestrator.wait_all_finalized()
        except Exception:
            pass
        try:
            await redis_client.aclose()
        except Exception:
            pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        # The KeyboardInterrupt raised inside main() was already handled
        # above; this catches a second Ctrl+C during cleanup.
        print("\n[exited]")