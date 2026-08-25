"""SSE frame encoding (RFC standard format).

Frame structure:
    id: {stream_id}:{seq}
    event: {event_type}
    data: {json single or multiple lines}

Discipline:
- \r\n / \n / \r within data are split into multiple data: lines (prevents frame injection, browser compatibility)
- json.dumps with ensure_ascii=False, compact separators (golden test key order stability)
"""
from __future__ import annotations

import json
from dataclasses import dataclass

from orditect.stream.events import EventEnvelope, EventType

HEARTBEAT_COMMENT = ":ping"


@dataclass(frozen=True)
class SSEFrame:
    """A single SSE frame."""

    event: str
    data: str
    id: str | None = None
    comment: str | None = None

    def encode(self) -> bytes:
        """Encode to standard SSE byte stream."""
        lines: list[str] = []
        if self.comment is not None:
            for line in self.comment.splitlines() or [""]:
                lines.append(f":{line}" if line else ":")
            return ("\n".join(lines) + "\n\n").encode("utf-8")

        if self.id is not None:
            lines.append(f"id: {self.id}")
        lines.append(f"event: {self.event}")
        # data multi-line splitting: normalize \r\n / \r to \n first, then output line by line
        normalized = self.data.replace("\r\n", "\n").replace("\r", "\n")
        for line in normalized.split("\n"):
            lines.append(f"data: {line}")
        return ("\n".join(lines) + "\n\n").encode("utf-8")


def frame_from_envelope(envelope: EventEnvelope, event_type: EventType) -> SSEFrame:
    """Envelope → SSE frame."""
    return SSEFrame(
        event=event_type.value,
        data=json.dumps(
            envelope.to_payload(),
            ensure_ascii=False,
            separators=(",", ":"),
        ),
        id=envelope.sse_id(),
    )


def heartbeat_frame() -> SSEFrame:
    """Heartbeat comment frame: prevents proxy buffer timeout, not part of business event stream."""
    return SSEFrame(event="", data="", comment=HEARTBEAT_COMMENT)


def encode_envelope(envelope: EventEnvelope, event_type: EventType) -> bytes:
    return frame_from_envelope(envelope, event_type).encode()


def encode_heartbeat() -> bytes:
    return heartbeat_frame().encode()