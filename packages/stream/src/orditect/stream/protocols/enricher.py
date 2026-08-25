"""Enricher protocol: rich-media placeholder resolution (vector DB retrieval / AI generation, etc.)."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol

from orditect.stream.events import PlaceholderState


@dataclass
class EnrichRequest:
    """Enrich request."""

    placeholder_id: str
    context_text: str          # marker 前提取的上下文（提取策略见 config）
    stream_id: str
    stage: str | None = None
    ext: dict[str, Any] = field(default_factory=dict)


@dataclass
class EnrichResult:
    """Enrich result."""

    url: str
    state: PlaceholderState = PlaceholderState.RESOLVED
    meta: dict[str, Any] = field(default_factory=dict)


class EnricherProtocol(Protocol):
    """Enricher protocol: resolves placeholders to actual resource URLs."""

    async def resolve(self, request: EnrichRequest) -> EnrichResult:
        """Resolve placeholder.

        Raises:
            EnrichError / Exception: resolution failure (manager will convert to failed state)
        """
        ...