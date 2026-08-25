"""Webhook callback: HTTP POST to specified URL."""
import logging
from typing import Dict, Any, Optional

import httpx

from orditect.flow.protocols.callback import CallbackProtocol

logger = logging.getLogger(__name__)


class WebhookCallback(CallbackProtocol):
    """Webhook callback (HTTP POST to specified URL)

        Responsibilities:
        - Sends HTTP POST request to specified URL on task success/failure/progress change
        - Supports custom headers
        - Supports timeout control
        - Logs failures (does not affect main flow)

        Usage example:
            callback = WebhookCallback(
                url="https://client.example.com/webhook",
                headers={"Authorization": "Bearer token123"},
                timeout=10.0,
            )

            # Automatically called on task success
            await callback.on_success(task_id="task-123", result={"data": "..."})
        """

    def __init__(
            self,
            url: str,
            headers: Optional[Dict[str, str]] = None,
            timeout: float = 10.0,
            retry_on_failure: bool = False,
    ):
        """
                Args:
                    url: Webhook URL
                    headers: Custom request headers (optional)
                    timeout: Request timeout in seconds
                    retry_on_failure: Whether to retry on failure (default False, to avoid blocking main flow)
                """
        self.url = url
        self.headers = headers or {}
        self.timeout = timeout
        self.retry_on_failure = retry_on_failure

    async def _send_webhook(self, payload: Dict[str, Any]) -> None:
        """Send webhook request.

                Args:
                    payload: Request body (JSON)
                """
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    self.url,
                    json=payload,
                    headers=self.headers,
                    timeout=self.timeout,
                )
                response.raise_for_status()
                logger.info(f"Webhook sent successfully: {self.url} (status: {response.status_code})")
        except httpx.HTTPError as e:
            logger.error(f"Webhook failed: {self.url}, error: {e}", exc_info=True)
            if self.retry_on_failure:
                # optional: retry after failure (simple retry once)
                try:
                    logger.info(f"Retrying webhook: {self.url}")
                    async with httpx.AsyncClient() as client:
                        response = await client.post(
                            self.url,
                            json=payload,
                            headers=self.headers,
                            timeout=self.timeout,
                        )
                        response.raise_for_status()
                        logger.info(f"Webhook retry succeeded: {self.url}")
                except httpx.HTTPError as retry_error:
                    logger.error(f"Webhook retry failed: {self.url}, error: {retry_error}")

    async def on_success(self, task_id: str, result: Dict[str, Any]) -> None:
        """Task success callback."""
        payload = {
            "task_id": task_id,
            "status": "succeeded",
            "result": result,
        }
        await self._send_webhook(payload)

    async def on_failure(self, task_id: str, error: Exception) -> None:
        """Task failure callback."""
        payload = {
            "task_id": task_id,
            "status": "failed",
            "error": str(error),
            "error_type": type(error).__name__,
        }
        await self._send_webhook(payload)

    async def on_progress(self, task_id: str, progress: float) -> None:
        """Task progress callback."""
        payload = {
            "task_id": task_id,
            "status": "running",
            "progress": progress,
        }
        await self._send_webhook(payload)

    async def on_status_change(self, task_id: str, old_status: str, new_status: str) -> None:
        """Task status change callback."""
        payload = {
            "task_id": task_id,
            "old_status": old_status,
            "new_status": new_status,
        }
        await self._send_webhook(payload)