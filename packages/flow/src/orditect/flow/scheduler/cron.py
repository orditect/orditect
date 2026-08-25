"""Cron-like scheduler: scheduled task scheduling similar to cron."""
import logging
from typing import Dict, List, Optional
from datetime import datetime

from croniter import croniter

from orditect.flow.protocols.scheduler import SchedulerProtocol

logger = logging.getLogger(__name__)


class CronScheduler(SchedulerProtocol):
    """Cron-like scheduler (similar to cron).

    Responsibilities:
    - Schedule tasks using cron expressions
    - Support daily, hourly, minutely, and other scheduled tasks
    - Automatically calculate the next execution time

    Usage example:
        scheduler = CronScheduler()

        # Execute at 2 AM every day
        await scheduler.schedule("task-1", cron_expr="0 2 * * *")

        # Execute every hour
        await scheduler.schedule("task-2", cron_expr="0 * * * *")

        # Execute every 5 minutes
        await scheduler.schedule("task-3", cron_expr="*/5 * * * *")

        # Get due tasks
        due_tasks = await scheduler.get_due_tasks()
    """

    def __init__(self):
        """Initialize the cron scheduler."""
        self._jobs: Dict[str, tuple[str, datetime]] = {}  # task_id -> (cron_expr, next_run)

    async def schedule(
            self,
            task_id: str,
            cron_expr: str,
            **kwargs,
    ) -> None:
        """Schedule a task with cron timing.

        Args:
            task_id: Task ID
            cron_expr: Cron expression (e.g. "0 2 * * *" for 2 AM every day)
            **kwargs: Other arguments (ignored)
        """
        try:
            cron = croniter(cron_expr, datetime.now())
            next_run = cron.get_next(datetime)
            self._jobs[task_id] = (cron_expr, next_run)
            logger.info(f"Task scheduled: {task_id} (cron: {cron_expr}, next_run: {next_run})")
        except Exception as e:
            logger.error(f"Invalid cron expression: {cron_expr}, error: {e}")
            raise ValueError(f"Invalid cron expression: {cron_expr}") from e

    async def cancel(self, task_id: str) -> None:
        """Cancel scheduling.

        Args:
            task_id: Task ID
        """
        if task_id in self._jobs:
            del self._jobs[task_id]
            logger.info(f"Task cancelled: {task_id}")

    async def get_ready_tasks(self, limit: int = 10) -> List[str]:
        """Get due tasks (next_run <= now).

        Args:
            limit: Maximum number of tasks to return

        Returns:
            List of due task IDs
        """
        now = datetime.now()
        due_tasks = []

        for task_id, (cron_expr, next_run) in list(self._jobs.items()):
            if len(due_tasks) >= limit:
                break

            if next_run <= now:
                due_tasks.append(task_id)

                # update next execution time
                cron = croniter(cron_expr, now)
                self._jobs[task_id] = (cron_expr, cron.get_next(datetime))

        logger.debug(f"Due tasks: {due_tasks}")
        return due_tasks

    async def get_due_tasks(self, limit: int = 10) -> List[str]:
        """Get due tasks (alias, same as get_ready_tasks)."""
        return await self.get_ready_tasks(limit)

    async def mark_completed(self, task_id: str) -> None:
        """Mark task as completed (not needed by this scheduler, but required by the interface)."""
        pass

    async def get_next_run(self, task_id: str) -> Optional[datetime]:
        """Get the next execution time of a task.

        Args:
            task_id: Task ID

        Returns:
            Next execution time, or None if the task does not exist.
        """
        if task_id in self._jobs:
            return self._jobs[task_id][1]
        return None