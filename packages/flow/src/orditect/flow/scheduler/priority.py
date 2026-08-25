"""Priority scheduler: high-priority tasks execute first."""
import heapq
import logging
from typing import List, Optional
from datetime import datetime

from orditect.flow.protocols.scheduler import SchedulerProtocol

logger = logging.getLogger(__name__)


class PriorityScheduler(SchedulerProtocol):
    """Priority scheduler (high-priority tasks execute first).

    Responsibilities:
    - Manage tasks using a priority queue (heap)
    - High-priority tasks (smaller priority value) execute first
    - Support task cancellation

    Usage example:
        scheduler = PriorityScheduler()

        # Schedule tasks (smaller priority means higher priority)
        await scheduler.schedule("task-1", priority=10)  # Low priority
        await scheduler.schedule("task-2", priority=1)   # High priority

        # Get ready tasks (sorted by priority)
        ready_tasks = await scheduler.get_ready_tasks()
        # Returns ["task-2", "task-1"] (task-2 has higher priority)
    """

    def __init__(self):
        """Initialize the priority scheduler."""
        self._queue: List[tuple[int, str]] = []  # 优先队列（堆）：(priority, task_id)
        self._cancelled: set[str] = set()  # 已取消的任务

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
            priority: Priority (smaller value means higher priority)
            run_at: Scheduled execution time (not supported by this scheduler, ignored)
            dependencies: List of dependent task IDs (not supported by this scheduler, ignored)
        """
        if task_id in self._cancelled:
            self._cancelled.remove(task_id)

        heapq.heappush(self._queue, (priority, task_id))
        logger.info(f"Task scheduled: {task_id} (priority: {priority})")

    async def cancel(self, task_id: str) -> None:
        """Cancel scheduling.

        Args:
            task_id: Task ID
        """
        self._cancelled.add(task_id)
        logger.info(f"Task cancelled: {task_id}")

    async def get_ready_tasks(self, limit: int = 10) -> List[str]:
        """Get ready tasks (sorted by priority).

        Args:
            limit: Maximum number of tasks to return

        Returns:
            List of ready task IDs (sorted from highest to lowest priority)
        """
        ready = []

        while self._queue and len(ready) < limit:
            priority, task_id = heapq.heappop(self._queue)

            # skip cancelled tasks
            if task_id in self._cancelled:
                self._cancelled.remove(task_id)
                continue

            ready.append(task_id)

        logger.debug(f"Ready tasks: {ready}")
        return ready

    async def mark_completed(self, task_id: str) -> None:
        """Mark task as completed (not needed by this scheduler, but required by the interface)."""
        pass

    async def size(self) -> int:
        """Get the number of tasks in the queue."""
        return len(self._queue)