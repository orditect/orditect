"""AuditEvent: one entry in the append-only audit log.

Idempotency discipline (dual-residence key term):
- event_id is the idempotency key. Re-appending the same event_id must not
  create a duplicate record (hot path and cold path dedup by the same key).
- Reusing event_id with a different payload raises IdempotencyConflictError.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from pydantic import Field
from orditect.protocol.models._base import ContractModel


def _utc_now() -> datetime:
    """Current time as timezone-aware UTC (single clock discipline)."""
    return datetime.now(UTC)

class AuditEvent(ContractModel):
    """One audit log entry (append-only).

    Attributes:
        event_id: Idempotency key (unique identifier of this event).
        task_id: Associated task identifier.
        scope: Free-form scope tag (e.g. budget scope; business-defined).
        event_type: Opaque event type string (protocol embeds no vocabulary).
        source: Originating framework ("core" / "flow" / "stream" / custom).
        payload: Free-form event payload (business-defined).
        timestamp: Event occurrence time.
    """

    event_id: str
    task_id: str
    scope: str | None = None
    event_type: str = ""
    source: str = ""
    payload: dict[str, Any] = Field(default_factory=dict)  # noqa: RUF012
    timestamp: datetime = Field(default_factory=_utc_now)