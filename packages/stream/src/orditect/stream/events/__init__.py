"""Event protocol layer: schema frozen, golden test locked."""
from orditect.stream.events.types import (
    PROTOCOL_VERSION,
    DeltaKind,
    ErrorCode,
    EventType,
    PlaceholderState,
)
from orditect.stream.events.envelope import EventEnvelope
from orditect.stream.events.payloads import (
    make_delta,
    make_enrich_marker,
    make_enrich_placeholder,
    make_enrich_resolved,
    make_manifest,
    make_stage_end,
    make_stream_cancelled,  # 新增
    make_stream_end,
    make_stream_error,
    make_stream_start,
    ManifestPlaceholder,
    StageResultPayload,
)

__all__ = [
    "PROTOCOL_VERSION",
    "DeltaKind",
    "ErrorCode",
    "EventType",
    "PlaceholderState",
    "EventEnvelope",
    "make_delta",
    "make_enrich_marker",
    "make_enrich_placeholder",
    "make_enrich_resolved",
    "make_manifest",
    "make_stage_end",
    "make_stream_cancelled",
    "make_stream_end",
    "make_stream_error",
    "make_stream_start",
    "ManifestPlaceholder",
    "StageResultPayload",
]