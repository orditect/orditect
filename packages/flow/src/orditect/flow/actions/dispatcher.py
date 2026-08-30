"""ActionDispatcher: consumes action commands from a queue and executes them.

This is the flow-side counterpart of the UI adapter's ActionSinkAdapter.
The dispatcher polls the action queue, validates each command, delegates to
flow's public operation surface (orchestrator cancel / RecoveryService
rerun / resume), and writes execution receipts.

Design discipline (DD-013):
- Asynchronous: the dispatcher runs as a background coroutine; action
  producers (UI/HITL/MCP/agent) never touch the hot path directly.
- Idempotent: action_id is the dedup key; a repeated action_id is silently
  skipped (the receipt is already written).
- Best-effort: individual action failures are logged and recorded in the
  receipt; they never crash the dispatcher loop.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from orditect.flow.actions.models import ActionCommand, ActionQueue, ActionType
from orditect.flow.recovery import ReuseDecision
from collections import OrderedDict

logger = logging.getLogger(__name__)


class ActionDispatcher:
    """Consumes action commands and executes them via flow's public API.

    Args:
        queue: ActionQueue implementation (same instance the UI side writes to).
        orchestrator: TaskOrchestrator (for cancel / terminate).
        recovery: RecoveryService (for resume / rerun).
        poll_interval: seconds between queue polls when empty.
    """

    def __init__(
            self,
            queue: ActionQueue,
            orchestrator: Any,
            recovery: Any,
            *,
            poll_interval: float = 1.0,
            dedup_capacity: int = 10000,
    ) -> None:
        self._queue = queue
        self._orchestrator = orchestrator
        self._recovery = recovery
        self._poll_interval = poll_interval
        # Bounded LRU dedup window (v0.1.6): an unbounded dict leaks memory
        # over the process lifetime. Dedup is only guaranteed within the
        # most recent `dedup_capacity` action_ids; beyond the window a
        # repeated action_id re-executes (pause is idempotent-harmless;
        # retry/resume are not — production deployments must use a
        # persistent queue, see adapter-ui README).
        self._dedup_capacity = dedup_capacity
        self._seen: OrderedDict[str, None] = OrderedDict()
        self._running = False
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        """Start the dispatcher loop (idempotent)."""
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._loop())
        logger.info("ActionDispatcher started")

    async def stop(self) -> None:
        """Stop the dispatcher loop (idempotent)."""
        self._running = False
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        logger.info("ActionDispatcher stopped")

    async def _loop(self) -> None:
        """Main dispatch loop."""
        while self._running:
            command = await self._queue.dequeue(timeout=self._poll_interval)
            if command is None:
                continue
            await self._execute(command)

    def _mark_seen(self, action_id: str) -> bool:
        """Record an action_id in the bounded dedup window.

        Returns True when the id was already present (duplicate); False
        when it is newly admitted. Evicts the least-recently-used entry
        once the window is full.
        """
        if action_id in self._seen:
            self._seen.move_to_end(action_id)
            return True
        self._seen[action_id] = None
        if len(self._seen) > self._dedup_capacity:
            self._seen.popitem(last=False)
        return False

    async def _execute(self, command: ActionCommand) -> None:
        """Validate, execute, and write receipt for one command."""
        # Idempotency: skip already-seen action_ids (bounded window)
        if self._mark_seen(command.action_id):
            logger.debug(f"action already executed (dedup): {command.action_id}")
            return

        receipt: dict[str, Any] = {
            "action_id": command.action_id,
            "action_type": command.action_type.value,
            "status": "executed",
            "detail": "",
        }
        try:
            if command.action_type is ActionType.PAUSE:
                ok = await self._orchestrator.cancel(command.target_task_id)
                receipt["detail"] = (
                    "cancelled" if ok else "task not found or already terminal"
                )
                receipt["status"] = "executed" if ok else "rejected"

            elif command.action_type is ActionType.RETRY:
                if command.root_task_id is None:
                    receipt["status"] = "rejected"
                    receipt["detail"] = "retry requires root_task_id"
                else:
                    scope = command.scope or frozenset({command.target_task_id})
                    plan = await self._recovery.rerun(
                        command.root_task_id, scope=set(scope)
                    )
                    rerun_count = sum(
                        1 for v in plan.values() if v is ReuseDecision.RERUN
                    )
                    receipt["detail"] = f"rerun={rerun_count} of {len(plan)}"

            elif command.action_type is ActionType.RESUME:
                plan = await self._recovery.resume(command.target_task_id)
                reused = sum(
                    1 for v in plan.values() if v is ReuseDecision.REUSE
                )
                rerun = sum(
                    1 for v in plan.values() if v is ReuseDecision.RERUN
                )
                receipt["detail"] = f"reuse={reused}, rerun={rerun}"

        except Exception as e:
            logger.error(
                f"action execution failed: {command.action_id}, {e}",
                exc_info=True,
            )
            receipt["status"] = "failed"
            receipt["detail"] = str(e)

        # Write receipt (best-effort; queue implementations may or may not
        # support receipts)
        try:
            if hasattr(self._queue, "write_receipt"):
                await self._queue.write_receipt(command.action_id, receipt)
        except Exception as e:
            logger.warning(f"receipt write failed: {command.action_id}, {e}")