"""Stream result data model (independent module, avoids circular dependencies).
StreamResult is the shared data model between runner and finalizer,
exists independently to avoid circular dependencies.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from orditect.stream.pipeline import MarkerHit
from orditect.stream.stages import StageOutcome


@dataclass
class StreamResult:
    """Final aggregation of a substream (for manifest)."""

    stream_id: str
    stages: dict[str, StageOutcome] = field(default_factory=dict)
    hits: list[MarkerHit] = field(default_factory=list)
    errors: list[dict[str, Any]] = field(default_factory=list)