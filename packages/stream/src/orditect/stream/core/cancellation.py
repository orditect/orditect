"""Cancellation token: cancellation signal carrier throughout the stream lifecycle."""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field


@dataclass
class CancellationToken:
    """Cancellation token (thread-safe, idempotent).

    Used to mark stream cancellation, throughout the entire stream lifecycle:
    - LLM source checks token to stop output (but continues consuming)
    - Enricher checks token to cancel tasks
    - StreamRunner checks token to emit cancellation event
    """

    _event: asyncio.Event = field(default_factory=asyncio.Event)
    _reason: str | None = None
    _cancelled_at: float | None = None

    def cancel(self, reason: str | None = None) -> None:
        """Mark cancelled (idempotent).

        Args:
            reason: cancellation reason (recorded in event)
        """
        if not self._event.is_set():
            self._reason = reason
            self._cancelled_at = time.time()
            self._event.set()

    def is_cancelled(self) -> bool:
        """Whether cancelled."""
        return self._event.is_set()

    @property
    def reason(self) -> str | None:
        """Cancellation reason."""
        return self._reason

    @property
    def cancelled_at(self) -> float | None:
        """Cancellation timestamp."""
        return self._cancelled_at

    async def wait(self) -> None:
        """Wait for cancellation signal."""
        await self._event.wait()

    def throw_if_cancelled(self) -> None:
        """Raise exception if cancelled.

        Raises:
            StreamCancelledError: stream has been cancelled
        """
        if self.is_cancelled():
            from orditect.stream.exceptions import StreamCancelledError
            raise StreamCancelledError(self._reason or "cancelled by user")