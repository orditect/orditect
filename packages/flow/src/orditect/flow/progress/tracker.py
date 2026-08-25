"""Progress tracking layer: task progress tracking and reporting."""
import logging
from typing import Optional

from orditect.flow.protocols.storage import TaskStorageProtocol

logger = logging.getLogger(__name__)


class ProgressTracker:
    """Progress tracker (tracks task progress).

    Responsibilities:
    - Update task progress
    - Query task progress
    - Progress persistence (via storage)

    Usage example:
        tracker = ProgressTracker(storage)

        # Update progress
        await tracker.update_progress("task-123", 0.5)

        # Query progress
        progress = await tracker.get_progress("task-123")
        # Returns 0.5
    """

    def __init__(self, storage: TaskStorageProtocol):
        """
        Args:
            storage: Task storage
        """
        self.storage = storage

    async def update_progress(self, task_id: str, progress: float) -> None:
        """Update task progress.

        Args:
            task_id: Task ID
            progress: Progress (0.0 - 1.0)

        Raises:
            ValueError: Progress value is invalid
        """
        if not 0.0 <= progress <= 1.0:
            raise ValueError(f"Progress must be between 0.0 and 1.0, got {progress}")

        await self.storage.update_task(task_id, {"progress": progress})
        logger.debug(f"Progress updated: {task_id} -> {progress:.2%}")

    async def get_progress(self, task_id: str) -> float:
        """Get task progress.

        Args:
            task_id: Task ID

        Returns:
            Progress (0.0 - 1.0)
        """
        task = await self.storage.get_task(task_id)
        return task.get("progress", 0.0)

    async def increment_progress(self, task_id: str, delta: float) -> float:
        """Increment task progress.

        Args:
            task_id: Task ID
            delta: Increment (can be negative)

        Returns:
            New progress value
        """
        current = await self.get_progress(task_id)
        new_progress = max(0.0, min(1.0, current + delta))
        await self.update_progress(task_id, new_progress)
        return new_progress