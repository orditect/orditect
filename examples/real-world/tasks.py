"""Workflow tasks for the MVP demo (recursive composition).

The root task submits child tasks inside its own execute() — the flow
contextvar auto-injects parent_task_id, so the lineage tree is registered
with zero boilerplate.
"""

from __future__ import annotations

import asyncio

from orditect.flow import BaseBackEndTask, TaskOrchestrator

ROOT_TASK_ID = "pipeline-root"


class CollectTask(BaseBackEndTask):
    """Step 1: collect documents (pure local work)."""

    async def execute(self, task_id: str, **kwargs) -> dict:
        await asyncio.sleep(0.2)
        return {"docs": ["doc-alpha", "doc-beta"]}


class AnalyzeTask(BaseBackEndTask):
    """Step 2: governed LLM call through the bridge.

    Every call passes an explicit call_id — the dual-habitat idempotency
    key shared by the quota hot path and the audit cold path, so a retry
    with the same key never double-charges or double-audits.
    """

    def __init__(self, storage, llm) -> None:
        super().__init__(storage)
        self._llm = llm

    async def execute(self, task_id: str, **kwargs) -> dict:
        record = await self.storage.get_task(task_id)
        eid = record.get("execution_id", "")
        result = await self._llm.chat(
            messages=[{"role": "user", "content": "Analyze the collected docs"}],
            call_id=f"analyze-{task_id}-{eid}",
        )
        return {"analysis": result["choices"][0]["message"]["content"]}


class ReportTask(BaseBackEndTask):
    """Step 3: fails on its first generation on purpose.

    Used to demonstrate the HITL retry path: retry goes through
    reopen_task, which opens a NEW execution generation (T3-safe rerun,
    never a state regression).
    """

    def __init__(self, storage, llm, fail_flags: dict) -> None:
        super().__init__(storage)
        self._llm = llm
        self._fail_flags = fail_flags

    async def execute(self, task_id: str, **kwargs) -> dict:
        if self._fail_flags.pop(task_id, False):
            raise RuntimeError("simulated first-run failure (retry via HITL)")
        record = await self.storage.get_task(task_id)
        eid = record.get("execution_id", "")
        result = await self._llm.chat(
            messages=[{"role": "user", "content": "Write the final report"}],
            call_id=f"report-{task_id}-{eid}",
        )
        return {"report": result["choices"][0]["message"]["content"]}


class SlowTask(BaseBackEndTask):
    """HITL pause demo node: cooperative cancellation.

    Mirrors the core CancellationToken discipline: the task checks the
    cancel_requested flag at segment boundaries and interrupts itself by
    raising CancelledError; the executor then settles it as cancelled and
    writes the terminal snapshot.
    """

    def __init__(self, storage, steps: int = 5, step_delay: float = 0.6) -> None:
        super().__init__(storage)
        self._steps = steps
        self._step_delay = step_delay

    async def execute(self, task_id: str, **kwargs) -> dict:
        for _ in range(self._steps):
            record = await self.storage.get_task(task_id)
            if record.get("cancel_requested"):
                raise asyncio.CancelledError()
            await asyncio.sleep(self._step_delay)
        return {"slow": "finished"}


class PipelineTask(BaseBackEndTask):
    """Root task: submits the three child steps sequentially.

    Child submissions happen inside the executor's task context, so
    parent_task_id is injected automatically (recursive composition).
    """

    def __init__(self, storage, orchestrator: TaskOrchestrator, llm,
                 fail_flags, step_timeout: float = 300.0) -> None:
        super().__init__(storage)
        self._orchestrator = orchestrator
        self._llm = llm
        self._fail_flags = fail_flags
        self._step_timeout = step_timeout

    async def execute(self, task_id: str, **kwargs) -> dict:
        collect = await self._orchestrator.submit(
            CollectTask(self.storage), task_id="collect"
        )
        await self._orchestrator.wait_terminal(collect, timeout=self._step_timeout)
        analyze = await self._orchestrator.submit(
            AnalyzeTask(self.storage, self._llm), task_id="analyze"
        )
        await self._orchestrator.wait_terminal(analyze, timeout=self._step_timeout)
        report = await self._orchestrator.submit(
            ReportTask(self.storage, self._llm, self._fail_flags),
            task_id="report",
        )
        record = await self._orchestrator.wait_terminal(report, timeout=self._step_timeout)
        return {"report_status": record["status"]}

def make_task_factory(storage, llm, fail_flags, orchestrator=None):
    """Build the task_factory required by RecoveryService."""

    async def factory(task_id: str):
        if task_id == ROOT_TASK_ID:
            if orchestrator is None:
                raise KeyError(f"pipeline-root needs an orchestrator to rebuild")
            return PipelineTask(storage, orchestrator, llm, fail_flags)
        if task_id == "collect":
            return CollectTask(storage)
        if task_id == "analyze":
            return AnalyzeTask(storage, llm)
        if task_id == "report":
            return ReportTask(storage, llm, fail_flags)
        if task_id == "slow":
            return SlowTask(storage)
        raise KeyError(f"unknown task_id: {task_id}")

    return factory