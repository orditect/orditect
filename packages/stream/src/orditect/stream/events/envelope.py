"""Event envelope: frame-level structure, fields fixed and closed.

Discipline:
- Only 6 top-level fields (v/stream_id/stage/seq/ts/data), no new fields allowed
- The only extension point for business is data.ext
"""
from __future__ import annotations

import time
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from orditect.stream.events.types import PROTOCOL_VERSION


class EventEnvelope(BaseModel):
    """Event envelope"""

    model_config = ConfigDict(frozen=True)

    v: int = PROTOCOL_VERSION
    stream_id: str
    stage: str | None = None
    seq: int
    ts: float = Field(default_factory=time.time)
    data: dict[str, Any]

    def sse_id(self) -> str:
        """SSE id field: resume checkpoint anchor (v1 reserved)."""
        return f"{self.stream_id}:{self.seq}"

    def to_payload(self) -> dict[str, Any]:
        """Serialize to dict for SSE data. Omit the stage key when it is None."""
        out: dict[str, Any] = {
            "v": self.v,
            "stream_id": self.stream_id,
            "seq": self.seq,
            "ts": self.ts,
            "data": self.data,
        }
        if self.stage is not None:
            out = {
                "v": self.v,
                "stream_id": self.stream_id,
                "stage": self.stage,
                "seq": self.seq,
                "ts": self.ts,
                "data": self.data,
            }
        return out