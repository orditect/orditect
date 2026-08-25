"""SSE frame → EventEnvelope deserialization (reuses events/ schema)."""
from __future__ import annotations

import json
from typing import Any

from orditect.stream.events import EventEnvelope


def envelope_from_frame(event: str, data: str, frame_id: str | None = None) -> EventEnvelope:
    """SSE frame triple → EventEnvelope.

    Args:
        event: event field value (event type)
        data: data field, multi-line reassembled JSON string
        frame_id: id field value ({stream_id}:{seq}, optional for validation)
    """
    payload: dict[str, Any] = json.loads(data)
    return EventEnvelope(
        v=payload.get("v", 1),
        stream_id=payload["stream_id"],
        stage=payload.get("stage"),
        seq=payload["seq"],
        ts=payload["ts"],
        data=payload["data"],
    )