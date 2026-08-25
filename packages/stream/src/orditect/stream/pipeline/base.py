"""Pipeline middleware base classes.

Two levels of processing:
- TextMiddleware:      AsyncIterable[str] -> AsyncIterable[str] (newline/tail)
- ChunkMiddleware:     AsyncIterable[SourceChunk] -> AsyncIterable[SourceChunk] (thinking/marker)

Each middleware has single responsibility, stateless and testable; composition is done by the runner.
"""
from __future__ import annotations

from collections.abc import AsyncIterable, AsyncIterator
from typing import Protocol

from orditect.stream.protocols import SourceChunk


class TextMiddleware(Protocol):
    def process(self, chunks: AsyncIterable[str]) -> AsyncIterator[str]: ...


class ChunkMiddleware(Protocol):
    def process(self, chunks: AsyncIterable[SourceChunk]) -> AsyncIterator[SourceChunk]: ...


async def aiter_from_iterable(items):
    """Synchronous iterable → async iterator (for testing/direct use)."""
    for item in items:
        yield item