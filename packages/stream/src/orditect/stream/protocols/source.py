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

    async def stream(
        self,
        request: SourceRequest,
        cancel_token: Any = None,
    ) -> AsyncIterator[SourceChunk]:
        """Stream deltas.

        Args:
            request: Source request (business-defined payload)
            cancel_token: Optional cancellation token passed by the caller
                (e.g. StageRunner forwards the substream's token). Duck-typed:
                the source may treat it as opaque — check `is_cancelled()`
                (sync or async) or ignore it entirely. Added v0.1.x: the
                parameter already existed at every call site and in every
                implementation (StageRunner, GovernedLLMClient, test doubles);
                this declaration catches up with reality.

        Yields:
            SourceChunk deltas; ends with a finish=True chunk (or natural end)
        """
        ...
        if False:
            yield  # pragma: no cover - 让 Protocol 方法成为 generator 类型