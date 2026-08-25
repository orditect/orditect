"""DisconnectMonitor: disconnection strategy executor.

Policy behaviors:
- cancel:   immediately trigger on_cancel callback (cascading cancel executor/enrich/close mux)
- grace:    start GraceBuffer + grace_period timer; reconnect within period → drain buffer;
            timeout → fallback to cancel
- continue: no intervention (run to completion, store manifest, refetch to recover)

Runner-side responsibility: upon disconnection detection (fastapi layer), call notify_disconnect();
upon reconnection, call notify_reconnect().
"""
from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

from orditect.stream.config import DisconnectPolicy, StreamConfig
from orditect.stream.disconnect.grace import GraceBuffer
from orditect.stream.events import EventEnvelope, EventType

OnCancelFn = Callable[[], Awaitable[None]]


class DisconnectMonitor:
    """Disconnection monitor."""

    def __init__(
        self,
        config: StreamConfig,
        on_cancel: OnCancelFn,
        grace_buffer: GraceBuffer | None = None,
    ):
        self._cfg = config
        self._on_cancel = on_cancel
        self._buffer = grace_buffer or GraceBuffer()
        self._disconnected = False
        self._timer_task: asyncio.Task | None = None
        self._cancelled = False

    @property
    def buffer(self) -> GraceBuffer:
        return self._buffer

    @property
    def is_disconnected(self) -> bool:
        return self._disconnected

    @property
    def is_cancelled(self) -> bool:
        return self._cancelled

    @property
    def should_buffer(self) -> bool:
        """Whether events should be written to buffer currently (disconnected and grace policy and not cancelled)."""
        return (
            self._disconnected
            and self._cfg.on_disconnect is DisconnectPolicy.GRACE
            and not self._cancelled
        )

    async def notify_disconnect(self) -> None:
        """Client disconnected."""
        if self._disconnected:
            return
        self._disconnected = True   # ← 必须先置位，grace 分支才生效

        policy = self._cfg.on_disconnect
        if policy is DisconnectPolicy.CANCEL:
            await self._do_cancel()
        elif policy is DisconnectPolicy.GRACE:
            self._start_grace_timer()
        # CONTINUE: no intervention

    async def notify_reconnect(self) -> tuple[list, bool]:
        """Client reconnected (grace policy): cancel timer, drain buffer.

        Returns:
            (List of BufferedEvent, whether there is a gap); for non-grace or already cancelled, returns ([], False)
        """
        if self._cfg.on_disconnect is not DisconnectPolicy.GRACE or self._cancelled:
            return [], False
        self._disconnected = False
        if self._timer_task is not None:
            self._timer_task.cancel()
            self._timer_task = None
        return await self._buffer.drain()

    def _start_grace_timer(self) -> None:
        async def _timeout() -> None:
            try:
                await asyncio.sleep(self._cfg.grace_period)
                await self._do_cancel()
            except asyncio.CancelledError:
                return

        self._timer_task = asyncio.create_task(_timeout())

    async def _do_cancel(self) -> None:
        if self._cancelled:
            return
        self._cancelled = True
        await self._on_cancel()

    async def close(self) -> None:
        """Clean up the timer (called when the stream ends normally)."""
        if self._timer_task is not None:
            self._timer_task.cancel()
            self._timer_task = None