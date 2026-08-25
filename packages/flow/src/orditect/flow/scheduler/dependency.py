"""Dependency scheduler: task B waits for task A to complete before executing."""
import logging
from typing import Dict, List, Set, Optional
from datetime import datetime

from orditect.flow.protocols.scheduler import SchedulerProtocol

logger = logging.getLogger(__name__)


class DependencyScheduler(SchedulerProtocol):
    """Dependency scheduler (task B waits for task A to complete before executing).

    Responsibilities:
    - Manage task dependencies
    - A task can only execute after all its dependencies are completed
    - Support task completion marking

    Usage example:
        scheduler = DependencyScheduler()

        # task-2 depends on task-1
        await scheduler.schedule("task-2", dependencies=["task-1"])

        # task-3 depends on task-1 and task-2
        await scheduler.schedule("task-3", dependencies=["task-1", "task-2"])

        # Mark task-1 as completed
        await scheduler.mark_completed("task-1")

        # Get ready tasks (task-2 can execute, task-3 is still waiting for task-2)
        ready_tasks = await scheduler.get_ready_tasks()
        # Returns ["task-2"]
    """

    def __init__(self):
        """Initialize the dependency scheduler."""
        self._dependencies: Dict[str, Set[str]] = {}  # task_id -> dependencies
        self._completed: Set[str] = set()  # 已完成的任务
        self._cancelled: Set[str] = set()  # 已取消的任务

    async def schedule(
            self,
            task_id: str,
            priority: int = 0,
            run_at: Optional[datetime] = None,
            dependencies: Optional[List[str]] = None,
    ) -> None:
        """Schedule a task.

        Args:
            task_id: Task ID
            priority: Priority (not supported by this scheduler, ignored)
            run_at: Scheduled execution time (not supported by this scheduler, ignored)
            dependencies: List of dependent task IDs
        """
        if task_id in self._cancelled:
            self._cancelled.remove(task_id)

        self._dependencies[task_id] = set(dependencies or [])
        logger.info(f"Task scheduled: {task_id} (dependencies: {dependencies})")

    async def cancel(self, task_id: str) -> None:
        """Cancel scheduling.

        Args:
            task_id: Task ID
        """
        self._cancelled.add(task_id)
        if task_id in self._dependencies:
            del self._dependencies[task_id]
        logger.info(f"Task cancelled: {task_id}")

    async def get_ready_tasks(self, limit: int = 10) -> List[str]:
        """Get ready tasks (all dependencies completed).

        Args:
            limit: Maximum number of tasks to return

        Returns:
            List of ready task IDs
        """
        ready = []

        for task_id, deps in self._dependencies.items():
            if len(ready) >= limit:
                break
            if task_id in self._completed or task_id in self._cancelled:
                continue
            if deps.issubset(self._completed):
                ready.append(task_id)

        logger.debug(f"Ready tasks: {ready}")
        return ready

    async def mark_completed(self, task_id: str) -> None:
        """Mark task as completed.

        Args:
            task_id: Task ID
        """
        self._completed.add(task_id)
        logger.info(f"Task marked as completed: {task_id}")

    async def get_dependencies(self, task_id: str) -> Set[str]:
        """Get the dependencies of a task.

        Args:
            task_id: Task ID

        Returns:
            Set of dependent task IDs
        """
        return self._dependencies.get(task_id, set())

    async def get_waiting_tasks(self) -> List[str]:
        """Get waiting tasks (tasks with incomplete dependencies).

        Returns:
            List of waiting task IDs
        """
        waiting = []

        for task_id, deps in self._dependencies.items():
            # skip completed or cancelled tasks
            if task_id in self._completed or task_id in self._cancelled:
                continue

            # has uncompleted dependencies
            if not deps.issubset(self._completed):
                waiting.append(task_id)

        return waiting