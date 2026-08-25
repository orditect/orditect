"""Progress reporter: notifies the client when progress changes."""
import logging
from typing import Optional

from orditect.flow.protocols.callback import CallbackProtocol

logger = logging.getLogger(__name__)


class ProgressReporter:
    """Progress reporter (notifies the client when progress changes).

    Responsibilities:
    - Invoke callback to notify client on progress changes
    - Support progress change threshold (avoid frequent notifications)

    Usage example:
        # Create callback
        callback = WebSocketCallback(connection_manager=manager)

        # Create progress reporter
        reporter = ProgressReporter(callback, threshold=0.1)

        # Report progress (only notifies when progress change exceeds 0.1)
        await reporter.report("task-123", 0.5)
    """

    def __init__(
            self,
            callback: CallbackProtocol,
            threshold: float = 0.0,
    ):
        """
        Args:
            callback: Callback interface (for notifying the client)
            threshold: Progress change threshold (only notify when progress change exceeds the threshold, default 0.0 means always notify)
        """
        self.callback = callback
        self.threshold = threshold
        self._last_progress: dict[str, float] = {}

    async def report(self, task_id: str, progress: float) -> None:
        """Report progress.

        Args:
            task_id: Task ID
            progress: Progress (0.0 - 1.0)
        """
        # check if progress change exceeds threshold
        last_progress = self._last_progress.get(task_id, -1.0)

        if last_progress < 0.0 or abs(progress - last_progress) >= self.threshold:
            await self.callback.on_progress(task_id, progress)
            self._last_progress[task_id] = progress
            logger.debug(f"Progress reported: {task_id} -> {progress:.2%}")

    async def report_increment(self, task_id: str, current: int, total: int) -> None:
        """Report incremental progress.

        Args:
            task_id: Task ID
            current: Current completed count
            total: Total count
        """
        if total <= 0:
            progress = 1.0
        else:
            progress = min(1.0, current / total)

        await self.report(task_id, progress)

    def reset(self, task_id: str) -> None:
        """Reset progress record.

        Args:
            task_id: Task ID
        """
        if task_id in self._last_progress:
            del self._last_progress[task_id]