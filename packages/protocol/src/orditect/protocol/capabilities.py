"""Capability declaration model for storage implementations.

Every storage implementation exposes a CapabilitySet declaring which
half-domains (sink/query per domain) it implements. Frameworks read this
at startup to probe features; unsupported operations must raise
UnsupportedCapabilityError (never silently degrade).
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class CapabilitySet(BaseModel):
    """Declares which storage domain half-interfaces an implementation provides.

    Each boolean corresponds to one half-domain (sink = write, query = read).
    Default False means "not supported"; callers must check before invoking.
    """

    # Content domain (ContentWriter / ContentReader)
    content_sink: bool = False
    content_query: bool = False

    # Audit domain (AuditWriter / AuditReader)
    audit_sink: bool = False
    audit_query: bool = False

    # Result domain (ResultWriter / ResultReader)
    result_sink: bool = False
    result_query: bool = False

    # Snapshot domain (SnapshotWriter / SnapshotReader)
    snapshot_sink: bool = False
    snapshot_query: bool = False

    # Protocol version compatibility range this implementation supports.
    # Format: PEP 440 version specifier, e.g. ">=0.1,<0.2".
    protocol_compat: str = Field(default=">=0.1,<0.2")

    model_config = {"frozen": True}

    def supports(self, half_domain: str) -> bool:
        """Check if a half-domain is supported.

        Args:
            half_domain: one of "content_sink", "content_query", "audit_sink",
                "audit_query", "result_sink", "result_query",
                "snapshot_sink", "snapshot_query".

        Raises:
            ValueError: unknown half-domain name (programming error, explicit).
        """
        if half_domain not in _HALF_DOMAINS:
            raise ValueError(f"unknown half-domain: {half_domain!r}")
        return getattr(self, half_domain)


_HALF_DOMAINS = frozenset({
    "content_sink", "content_query",
    "audit_sink", "audit_query",
    "result_sink", "result_query",
    "snapshot_sink", "snapshot_query",
})


__all__ = ["CapabilitySet"]