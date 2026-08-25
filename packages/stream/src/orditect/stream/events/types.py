"""Event type enumeration. Freeze discipline: new event types can only be appended, renaming/deletion is prohibited (locked by golden test)."""
from __future__ import annotations

from enum import Enum

PROTOCOL_VERSION = 1


class EventType(str, Enum):
    """Event types (SSE event: field value)."""

    STREAM_START = "stream.start"
    STREAM_DELTA = "stream.delta"
    ENRICH_MARKER = "enrich.marker"
    ENRICH_PLACEHOLDER = "enrich.placeholder"
    ENRICH_RESOLVED = "enrich.resolved"
    STAGE_END = "stage.end"
    STREAM_MANIFEST = "stream.manifest"
    STREAM_END = "stream.end"
    STREAM_ERROR = "stream.error"
    STREAM_CANCELLED = "stream.cancelled"  # 新增：用户主动打断
    # heartbeat is not a business event, does not go through envelope; SSE comment frame direct output


class DeltaKind(str, Enum):
    """data.kind for stream.delta."""

    CONTENT = "content"
    THINKING = "thinking"
    REFERENCES = "references"


class PlaceholderState(str, Enum):
    """Placeholder state machine."""

    PENDING = "pending"          # 已派发，未完成（含 settle 超时进 manifest 的）
    RESOLVED = "resolved"        # 已解析出真实 url
    FAILED = "failed"            # 解析失败，使用 fallback


class ErrorCode(str, Enum):
    """data.code for stream.error."""

    SOURCE_ERROR = "SOURCE_ERROR"
    ENRICH_ERROR = "ENRICH_ERROR"
    TIMEOUT = "TIMEOUT"
    CANCELLED = "CANCELLED"
    BACKPRESSURE = "BACKPRESSURE"
    UPSTREAM_INTERRUPTED = "UPSTREAM_INTERRUPTED"
    INTERNAL = "INTERNAL"