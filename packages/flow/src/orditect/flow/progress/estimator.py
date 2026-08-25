"""Progress estimator: estimate task progress based on historical data."""
import logging
import time
from typing import Optional, List

logger = logging.getLogger(__name__)


class ProgressEstimator:
    """Progress estimator (estimate task progress based on historical data).

    Responsibilities:
    - Estimate progress based on current step
    - Estimate progress based on elapsed time
    - Predict remaining time based on historical data

    Usage example:
        estimator = ProgressEstimator()

        # Estimate progress by steps
        progress = await estimator.estimate_by_steps(current_step=3, total_steps=10)
        # Returns 0.3

        # Estimate progress by time
        progress = await estimator.estimate_by_time(elapsed=30.0, avg_duration=100.0)
        # Returns 0.3
    """

    def __init__(self):
        """Initialize the progress estimator."""
        self._start_times: dict[str, float] = {}

    async def estimate_by_steps(
            self,
            current_step: int,
            total_steps: int,
    ) -> float:
        """Estimate progress based on steps.

        Args:
            current_step: Current step (starting from 0)
            total_steps: Total number of steps

        Returns:
            Progress (0.0 - 1.0)
        """
        if total_steps <= 0:
            return 1.0

        progress = min(1.0, current_step / total_steps)
        return progress

    async def estimate_by_time(
            self,
            elapsed: float,
            avg_duration: float,
    ) -> float:
        """Estimate progress based on time.

        Args:
            elapsed: Elapsed time in seconds
            avg_duration: Average execution time in seconds

        Returns:
            Progress (0.0 - 1.0)
        """
        if avg_duration <= 0:
            return 1.0

        progress = min(1.0, elapsed / avg_duration)
        return progress

    async def estimate_by_items(
            self,
            completed_items: int,
            total_items: int,
    ) -> float:
        """Estimate progress based on completed items.

        Args:
            completed_items: Number of completed items
            total_items: Total number of items

        Returns:
            Progress (0.0 - 1.0)
        """
        if total_items <= 0:
            return 1.0

        progress = min(1.0, completed_items / total_items)
        return progress

    async def start_timing(self, task_id: str) -> None:
        """Start timing.

        Args:
            task_id: Task ID
        """
        self._start_times[task_id] = time.time()

    async def get_elapsed(self, task_id: str) -> float:
        """Get elapsed time.

        Args:
            task_id: Task ID

        Returns:
            Elapsed time in seconds
        """
        if task_id not in self._start_times:
            return 0.0

        return time.time() - self._start_times[task_id]

    async def estimate_remaining_time(
            self,
            task_id: str,
            progress: float,
    ) -> Optional[float]:
        """Estimate remaining time.

        Args:
            task_id: Task ID
            progress: Current progress (0.0 - 1.0)

        Returns:
            Remaining time in seconds, or None if estimation is not possible
        """
        if progress <= 0.0:
            return None

        elapsed = await self.get_elapsed(task_id)
        if elapsed <= 0.0:
            return None

        total_estimated = elapsed / progress
        remaining = total_estimated - elapsed

        return max(0.0, remaining)