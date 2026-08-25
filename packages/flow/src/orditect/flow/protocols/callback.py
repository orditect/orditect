"""Abstract interface for callbacks."""
from typing import Protocol, Dict, Any


class CallbackProtocol(Protocol):
    """Callback protocol.

    Responsibilities:
    - Notify external systems on task success/failure/progress changes
    - Support Webhook, WebSocket, custom callbacks
    """

    async def on_success(self, task_id: str, result: Dict[str, Any]) -> None:
        """Callback on task success.

        Args:
            task_id: Task ID
            result: Task result
        """
        ...

    async def on_failure(self, task_id: str, error: Exception) -> None:
        """Callback on task failure.

        Args:
            task_id: Task ID
            error: Exception information
        """
        ...

    async def on_progress(self, task_id: str, progress: float) -> None:
        """Callback on task progress.

        Args:
            task_id: Task ID
            progress: Progress (0.0 - 1.0)
        """
        ...

    async def on_status_change(self, task_id: str, old_status: str, new_status: str) -> None:
        """Callback on task status change.

        Args:
            task_id: Task ID
            old_status: Old status
            new_status: New status
        """
        ...