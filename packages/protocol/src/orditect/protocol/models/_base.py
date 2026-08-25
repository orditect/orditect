"""Shared base for protocol data models.

Two cross-cutting disciplines:
- Frozen: models are immutable after construction (contract integrity).
- Compact: None-valued fields are omitted from serialized output
  (aligned with the stream event protocol's compactness rule).
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class ContractModel(BaseModel):
    """Base model for all orditect-protocol data models."""

    model_config = ConfigDict(
        frozen=True,
        # Omit None-valued fields from serialization output.
        # Callers reading serialized dicts must use .get() (key may be absent).
        ser_json_inf_nan="null",
    )

    def to_payload(self) -> dict:
        """Serialize to a compact dict (None fields omitted)."""
        return self.model_dump(exclude_none=True, mode="json")