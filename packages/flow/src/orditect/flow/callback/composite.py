"""Composite callback: execute multiple callbacks simultaneously."""
import logging
from typing import List, Dict, Any

from orditect.flow.protocols.callback import CallbackProtocol

logger = logging.getLogger(__name__)


class CompositeCallback(CallbackProtocol):
    """Composite callback (executes multiple callbacks simultaneously)

        Responsibilities:
        - Combines multiple callbacks into one
        - On task success/failure/progress change, calls all callbacks simultaneously
        - A single callback failure does not affect others

        Usage example:
            # Send both Webhook and WebSocket notifications simultaneously
            webhook_callback = WebhookCallback(url="https://...")
            websocket_callback = WebSocketCallback(connection_manager=manager)

            composite = CompositeCallback(callbacks=[
                webhook_callback,
                websocket_callback,
            ])

            # On task success, calls both webhook and websocket
            await composite.on_success(task_id="task-123", result={"data": "..."})
        """

    def __init__(self, callbacks: List[CallbackProtocol]):
        """
                Args:
                    callbacks: List of callbacks
                """
        if not callbacks:
            raise ValueError("CompositeCallback requires at least one callback")
        self.callbacks = callbacks

    async def on_success(self, task_id: str, result: Dict[str, Any]) -> None:
        """Task success callback."""
        for callback in self.callbacks:
            try:
                await callback.on_success(task_id, result)
            except Exception as e:
                logger.error(
                    f"Callback failed in CompositeCallback.on_success: {e}",
                    exc_info=True,
                )

    async def on_failure(self, task_id: str, error: Exception) -> None:
        """Task failure callback."""
        for callback in self.callbacks:
            try:
                await callback.on_failure(task_id, error)
            except Exception as e:
                logger.error(
                    f"Callback failed in CompositeCallback.on_failure: {e}",
                    exc_info=True,
                )

    async def on_progress(self, task_id: str, progress: float) -> None:
        """Task progress callback."""
        for callback in self.callbacks:
            try:
                await callback.on_progress(task_id, progress)
            except Exception as e:
                logger.error(
                    f"Callback failed in CompositeCallback.on_progress: {e}",
                    exc_info=True,
                )

    async def on_status_change(self, task_id: str, old_status: str, new_status: str) -> None:
        """Task status change callback."""
        for callback in self.callbacks:
            try:
                await callback.on_status_change(task_id, old_status, new_status)
            except Exception as e:
                logger.error(
                    f"Callback failed in CompositeCallback.on_status_change: {e}",
                    exc_info=True,
                )