"""Delayed scheduler (skeleton implementation — see module docstring warning).

⚠️ This scheduler is currently a skeleton implementation: schedule() only creates a sleep coroutine,
logs when expired, but does NOT trigger any task execution callback; get_ready_tasks() always returns an empty list.

For production delayed scheduling, use an external scheduler (APScheduler / Celery Beat)
to drive TaskOrchestrator.submit(); this class only retains the interface contract.
"""
import asyncio
import logging
from typing import Dict, Optional
from datetime import datetime

from orditect.flow.protocols.scheduler import SchedulerProtocol

logger = logging.getLogger(__name__)


class DelayedScheduler(SchedulerProtocol):
    """Delayed scheduler (skeleton implementation — see module docstring warning).

    Contract explicit: schedule() only logs on expiry, does not trigger execution;
    get_ready_tasks() always returns an empty list. Callers must not rely on this class
    to produce actual task scheduling behavior.
    """

    def __init__(self):
        self._tasks: Dict[str, asyncio.Task] = {}

    async def schedule(
            self,
            task_id: str,
            delay: float,
            **kwargs,
    ) -> None:
        """Delayed scheduling (⚠️ Skeleton: on expiry only logs, does not trigger task execution)."""

        async def delayed_task():
            await asyncio.sleep(delay)
            logger.warning(
                f"DelayedScheduler is a skeleton implementation: "
                f"task {task_id} delay elapsed but NO execution triggered. "
                f"Use external scheduler (APScheduler/Celery Beat) for production."
            )

        if task_id in self._tasks:
            self._tasks[task_id].cancel()

        self._tasks[task_id] = asyncio.create_task(delayed_task())
        logger.info(f"Task scheduled (skeleton, no execution): {task_id} (delay: {delay}s)")

    async def schedule_at(
            self,
            task_id: str,
            run_at: datetime,
    ) -> None:
        """Scheduled at a specific time (⚠️ skeleton, same as schedule)."""
        now = datetime.now()
        if run_at > now:
            delay = (run_at - now).total_seconds()
            await self.schedule(task_id, delay=delay)
        else:
            logger.warning(f"run_at is in the past: {run_at}")

    async def cancel(self, task_id: str) -> None:
        """Cancel scheduling."""
        if task_id in self._tasks:
            self._tasks[task_id].cancel()
            del self._tasks[task_id]
            logger.info(f"Task cancelled: {task_id}")

    async def get_ready_tasks(self, limit: int = 10) -> list[str]:
        """⚠️ Skeleton implementation: always returns an empty list (delayed tasks never enter the ready queue)."""
        return []

    async def mark_completed(self, task_id: str) -> None:
        """Mark task as completed (skeleton, no-op)."""
        pass