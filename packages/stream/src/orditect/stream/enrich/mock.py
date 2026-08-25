"""Mock enricher: simulated vector database retrieval for development and testing."""
from __future__ import annotations

import asyncio

from orditect.stream.core import CancellationToken
from orditect.stream.events import PlaceholderState
from orditect.stream.protocols import EnrichRequest, EnrichResult


class MockVectorEnricher:
    """Mock vector retrieval enricher.

        - Fixed latency seconds
        - Returns a fixed OSS URL (can be distinguished by context hash)
        - fail_on_context: raise exception when context contains specified substring (for testing failure paths)
        """

    def __init__(
        self,
        latency: float = 0.05,
        url_template: str = "https://oss.example.com/mock/{placeholder_id}.jpg",
        fail_on_context: str | None = None,
    ):
        self._latency = latency
        self._url_template = url_template
        self._fail_on = fail_on_context

    async def resolve(
        self,
        request: EnrichRequest,
        cancel_token: CancellationToken | None = None,  # 新增
    ) -> EnrichResult:
        """Resolve the placeholder.

        Args:
            request: enrich request
            cancel_token: cancellation token (optional, not used in mock)

        Returns:
            EnrichResult
        """
        await asyncio.sleep(self._latency)
        if self._fail_on and self._fail_on in request.context_text:
            raise RuntimeError(f"mock enrich failed on context: {request.context_text[:20]}")
        return EnrichResult(
            url=self._url_template.format(placeholder_id=request.placeholder_id),
            state=PlaceholderState.RESOLVED,
            meta={"source": "mock_vector"},
        )