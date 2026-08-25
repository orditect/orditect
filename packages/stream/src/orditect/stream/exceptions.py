"""Base class for all framework exceptions."""
from __future__ import annotations

from typing import Any


class TaskstreamError(Exception):
    """Base class for all framework exceptions."""


class ProtocolError(TaskstreamError):
    """Event protocol error (schema validation failure, illegal event type, etc.)."""


class StreamClosedError(TaskstreamError):
    """Write to a closed stream."""


class StreamCancelledError(TaskstreamError):
    """Stream cancelled (user-initiated interruption)."""


class SourceError(TaskstreamError):
    """LLM source generation or fetch failure."""


class EnrichError(TaskstreamError):
    """Enrich task dispatch or execution failure."""


class BackpressureError(TaskstreamError):
    """Triggered when queue is full under backpressure policy 'fail'."""


class StoreError(TaskstreamError):
    """Result store read/write failure."""

class StructuredStreamError(TaskstreamError):
    """Structured error that can be serialized as a stream.error event.

    Carries code/retryable; runner catches and converts to stream.error event for delivery instead of directly breaking the stream.
    """

    def __init__(
        self,
        code: str,
        message: str,
        *,
        retryable: bool = False,
        stage: str | None = None,
        ext: dict[str, Any] | None = None,
    ):
        super().__init__(message)
        self.code = code
        self.message = message
        self.retryable = retryable
        self.stage = stage
        self.ext = ext or {}