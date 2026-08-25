"""Abstract interface for task scheduling."""
from typing import Protocol, List, Optional
from datetime import datetime


class SchedulerProtocol(Protocol):
    """Task scheduling protocol.

    Responsibilities:
    - Task priority scheduling
    - Scheduled task scheduling
    - Delayed task scheduling
    - Dependency task scheduling
    """

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
            run_at: Scheduled execution time (optional)
            dependencies: List of dependent task IDs (optional)
        """
        ...

    async def cancel(self, task_id: str) -> None:
        """Cancel scheduling.

        Args:
            task_id: Task ID
        """
        ...

    async def get_ready_tasks(self, limit: int = 10) -> List[str]:
        """Get ready tasks (can be executed immediately).

        Args:
            limit: Maximum number of tasks to return

        Returns:
            List of ready task IDs
        """
        ...

    async def mark_completed(self, task_id: str) -> None:
        """Mark task as completed (for dependency scheduling).

        Args:
            task_id: Task ID
        """
        ...