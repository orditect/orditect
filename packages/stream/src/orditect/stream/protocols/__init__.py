"""Abstract interface layer: loosely coupled protocols."""
from orditect.stream.protocols.source import (
    LLMSourceProtocol,
    SourceChunk,
    SourceRequest,
)
from orditect.stream.protocols.enricher import (
    EnricherProtocol,
    EnrichRequest,
    EnrichResult,
)
from orditect.stream.protocols.store import (
    ResultStoreProtocol,
    JournalProtocol,
)
from orditect.stream.protocols.hooks import StreamHooks
from orditect.stream.protocols.governor import ResourceGovernorProtocol  # 新增

__all__ = [
    "LLMSourceProtocol",
    "SourceChunk",
    "SourceRequest",
    "EnricherProtocol",
    "EnrichRequest",
    "EnrichResult",
    "ResultStoreProtocol",
    "JournalProtocol",
    "StreamHooks",
    "ResourceGovernorProtocol",  # 新增
]