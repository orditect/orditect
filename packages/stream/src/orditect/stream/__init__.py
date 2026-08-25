"""orditect-stream — synchronous streaming rich-media output framework (output facet of the triad).

Core features:
- Standard SSE protocol: stream.delta / enrich.* / stage.end / stream.manifest
- Multi-stream multiplexing: one request can merge up to max_id substreams for output
- Rich-media placeholders: in-stream image markers → placeholders → backfill (settle window / manifest delegation)
- Thinking modes: three levels — inline / separate / suppress
- Disconnect policy: cancel / grace / continue
- Loose coupling: can be used standalone or integrated with orditect-flow/taskbase
- Resource status query: StreamGovernorManager (semaphore visualization enhancement)
- Dual cancellation modes: cancel() graceful interrupt / cancel(force=True) force release semaphore
"""
from importlib.metadata import PackageNotFoundError, version as _pkg_version

try:
    __version__ = _pkg_version("orditect-stream")
except PackageNotFoundError:  # 未安装的开发环境（裸 sys.path 导入）
    __version__ = "0.0.0.dev0"

from orditect.stream.exceptions import (
    TaskstreamError,
    ProtocolError,
    StreamClosedError,
    StreamCancelledError,
    SourceError,
    EnrichError,
    BackpressureError,
    StoreError,
    StructuredStreamError,
)
from orditect.stream.config import (
    StreamConfig,
    ThinkingMode,
    DisconnectPolicy,
    EnrichMode,
    BackpressurePolicy,
    DEFAULT_CONFIG,
)
from orditect.stream.events import (
    PROTOCOL_VERSION,
    EventType,
    DeltaKind,
    PlaceholderState,
    ErrorCode,
    EventEnvelope,
)
from orditect.stream.sse import SSEFrame, SSEWriter, parse_last_event_id

from orditect.stream.runner import StreamRunner, StreamExecutor, StreamResult
from orditect.stream.stages import StageConfig, StageOutcome, SourceType, StageRunner
from orditect.stream.mux import StreamMux, SeqAllocator
from orditect.stream.enrich import EnrichManager, PlaceholderRegistry, PlaceholderRecord, MockVectorEnricher
from orditect.stream.finalizer import ManifestBuilder, FinalizerHook
from orditect.stream.disconnect import DisconnectMonitor, GraceBuffer
from orditect.stream.store import (
    MemoryResultStore,
    ProtocolResultStore,
    get_default_store,
    get_protocol_store,
)
from orditect.stream.protocols import (
    LLMSourceProtocol, SourceChunk, SourceRequest,
    EnricherProtocol, EnrichRequest, EnrichResult,
    ResultStoreProtocol, JournalProtocol, StreamHooks,
)
from orditect.stream.core import CancellationToken
from orditect.stream.protocols.governor import ResourceGovernorProtocol
from orditect.stream.stages import DEFAULT_STREAM_LLM_RESOURCE
from orditect.stream.events import make_stream_cancelled
from orditect.stream.governor import StreamGovernorManager


__all__ = [
    "__version__",
    # exception
    "TaskstreamError",
    "ProtocolError",
    "StreamClosedError",
    "StreamCancelledError",
    "SourceError",
    "EnrichError",
    "BackpressureError",
    "StoreError",
    "StructuredStreamError",
    # configuration
    "StreamConfig",
    "ThinkingMode",
    "DisconnectPolicy",
    "EnrichMode",
    "BackpressurePolicy",
    "DEFAULT_CONFIG",
    # event protocol
    "PROTOCOL_VERSION",
    "EventType",
    "DeltaKind",
    "PlaceholderState",
    "ErrorCode",
    "EventEnvelope",
    # SSE
    "SSEFrame",
    "SSEWriter",
    "parse_last_event_id",

    "StreamRunner", "StreamExecutor", "StreamResult",
    "StageConfig", "StageOutcome", "SourceType", "StageRunner",
    "StreamMux", "SeqAllocator",
    "EnrichManager", "PlaceholderRegistry", "PlaceholderRecord", "MockVectorEnricher",
    "ManifestBuilder", "FinalizerHook",
    "DisconnectMonitor", "GraceBuffer",
    "MemoryResultStore", "get_default_store",
    "ProtocolResultStore",
    "get_protocol_store",
    "LLMSourceProtocol", "SourceChunk", "SourceRequest",
    "EnricherProtocol", "EnrichRequest", "EnrichResult",
    "ResultStoreProtocol", "JournalProtocol", "StreamHooks",
    "CancellationToken",  # 新增
    "ResourceGovernorProtocol",  # 新增
    "StreamCancelledError",  # 新增（已在 exceptions.py 中）
    "DEFAULT_STREAM_LLM_RESOURCE",  # 新增
    "make_stream_cancelled",  # 新增
    "StreamGovernorManager",  # 新增（sem 可视化增强）
]