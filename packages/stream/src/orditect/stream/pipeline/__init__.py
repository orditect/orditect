"""Stream pipeline layer: composable middleware."""
from orditect.stream.pipeline.base import (
    TextMiddleware,
    ChunkMiddleware,
    aiter_from_iterable,
)
from orditect.stream.pipeline.newline import NewlineNormalizer
from orditect.stream.pipeline.thinking import ChunkSplitter, ThinkingDemux
from orditect.stream.pipeline.marker import MarkerDetector, MarkerHit, MarkedChunk
from orditect.stream.pipeline.tail import TailCleaner

__all__ = [
    "TextMiddleware",
    "ChunkMiddleware",
    "aiter_from_iterable",
    "NewlineNormalizer",
    "ChunkSplitter",
    "ThinkingDemux",
    "MarkerDetector",
    "MarkerHit",
    "MarkedChunk",
    "TailCleaner",
]