"""LLM source protocol: business decoupling boundary.

The framework does not understand prompt templates — the business wraps its own LLM calls into this protocol.
"""
from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import Any, Protocol


@dataclass
class SourceRequest:
    """Source request (framework does not interpret payload content, business-defined)."""

    payload: dict[str, Any] = field(default_factory=dict)


@dataclass
class SourceChunk:
    """Source delta chunk (structured, thinking is no longer a <think> string marker).

    A single chunk may carry only one field, or a combination; finish=True indicates the source has ended.
    """

    text: str | None = None
    thinking: str | None = None
    references: list[dict[str, Any]] | None = None
    finish: bool = False

    def is_empty(self) -> bool:
        return (
            self.text is None
            and self.thinking is None
            and self.references is None
            and not self.finish
        )


class LLMSourceProtocol(Protocol):
    """LLM source protocol."""

    async def stream(self, request: SourceRequest) -> AsyncIterator[SourceChunk]:
        """Stream deltas.

        Args:
            request: Source request (business-defined payload)

        Yields:
            SourceChunk deltas; ends with a finish=True chunk (or natural end)
        """
        ...
        if False:
            yield  # pragma: no cover - 让 Protocol 方法成为 generator 类型