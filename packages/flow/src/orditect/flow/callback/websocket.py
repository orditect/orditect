"""WebSocket callback: real-time push to frontend."""
import logging
from typing import Dict, Any, Optional, Protocol

from orditect.flow.protocols.callback import CallbackProtocol

logger = logging.getLogger(__name__)


class ConnectionManagerProtocol(Protocol):
    """WebSocket connection manager protocol.

        Defines the interface for WebSocket connection manager (implemented by FastAPI application).
        """

    async def broadcast(self, message: Dict[str, Any]) -> None:
        """Broadcast message to all connected clients.

                Args:
                    message: Message content (JSON)
                """
        ...

    async def send_to_client(self, client_id: str, message: Dict[str, Any]) -> None:
        """Send message to a specific client.

                Args:
                    client_id: Client ID
                    message: Message content (JSON)
                """
        ...


class WebSocketCallback(CallbackProtocol):
    """WebSocket callback (real-time push to frontend)

        Responsibilities:
        - Pushes task success/failure/progress updates to frontend via WebSocket in real time
        - Supports both broadcast (all clients) and unicast (specific client)
        - Logs failures (does not affect main flow)

        Usage example:
            # WebSocket connection manager in FastAPI application
            class ConnectionManager:
                def __init__(self):
                    self.active_connections: List[WebSocket] = []

                async def broadcast(self, message: Dict[str, Any]):
                    for connection in self.active_connections:
                        await connection.send_json(message)

            # Create callback
            manager = ConnectionManager()
            callback = WebSocketCallback(connection_manager=manager)

            # Automatically called on task progress change
            await callback.on_progress(task_id="task-123", progress=0.5)
        """

    def __init__(
            self,
            connection_manager: ConnectionManagerProtocol,
            client_id: Optional[str] = None,
    ):
        """
                Args:
                    connection_manager: WebSocket connection manager
                    client_id: Client ID (optional, if specified unicast, otherwise broadcast)
                """
        self.connection_manager = connection_manager
        self.client_id = client_id

    async def _send_message(self, message: Dict[str, Any]) -> None:
        """Send WebSocket message.

                Args:
                    message: Message content (JSON)
                """
        try:
            if self.client_id:
                # unicast
                await self.connection_manager.send_to_client(self.client_id, message)
                logger.debug(f"WebSocket message sent to client: {self.client_id}")
            else:
                # broadcast
                await self.connection_manager.broadcast(message)
                logger.debug("WebSocket message broadcasted")
        except Exception as e:
            logger.error(f"WebSocket callback failed: {e}", exc_info=True)

    async def on_success(self, task_id: str, result: Dict[str, Any]) -> None:
        """Task success callback."""
        message = {
            "type": "task_success",
            "task_id": task_id,
            "status": "succeeded",
            "result": result,
        }
        await self._send_message(message)

    async def on_failure(self, task_id: str, error: Exception) -> None:
        """Task failure callback."""
        message = {
            "type": "task_failure",
            "task_id": task_id,
            "status": "failed",
            "error": str(error),
            "error_type": type(error).__name__,
        }
        await self._send_message(message)

    async def on_progress(self, task_id: str, progress: float) -> None:
        """Task progress callback."""
        message = {
            "type": "task_progress",
            "task_id": task_id,
            "status": "running",
            "progress": progress,
        }
        await self._send_message(message)

    async def on_status_change(self, task_id: str, old_status: str, new_status: str) -> None:
        """Task status change callback."""
        message = {
            "type": "task_status_change",
            "task_id": task_id,
            "old_status": old_status,
            "new_status": new_status,
        }
        await self._send_message(message)