"""Neutral error taxonomy for orditect-protocol contracts.

Discipline:
- No storage dialect (no Redis/Postgres/S3 specific exception names).
- No HTTP / framework coupling (mapping to HTTP status codes is the caller's job).
- All contract violations raise explicit subclasses; silent no-op is forbidden.
"""

from __future__ import annotations


class ContractError(Exception):
    """Base class for all orditect-protocol contract errors.

    Named ContractError (not ProtocolError) to avoid collision with
    typing.Protocol semantics and Python's built-in exception naming.
    """


class UnsupportedCapabilityError(ContractError):
    """Raised when a storage implementation does not support a requested operation.

    This is the enforcement mechanism for the explicit-capability term:
    an implementation must raise this error (never silently no-op) when a
    method is called that it has not declared in its CapabilitySet.
    """


class ContentNotFoundError(ContractError):
    """Raised when a content pointer references a non-existent object."""


class SnapshotNotFoundError(ContractError):
    """Raised when a requested task snapshot does not exist."""


class TerminalStateViolationError(ContractError):
    """Raised when an attempt is made to mutate a terminal-state record.

    Terminal-state protection is unconditional on the storage side;
    this error makes the rejection explicit.
    """


class IdempotencyConflictError(ContractError):
    """Raised when an idempotency key is reused with a different payload.

    Reusing the same key with identical payload is a silent success (dedup);
    reusing with a different payload is a conflict and must be explicit.
    """

class InvalidQueryError(ContractError):
    """Raised when a query parameter is outside the contract mechanism
    whitelist (e.g. an unknown sort field or group_by value).

    Mechanism-layer error: carries no business semantics. Explicit rejection
    replaces silent fallback so that adapters of every backend family share
    one behavior (T8 spirit applied to parameters).
    """

__all__ = [
    "ContractError",
    "UnsupportedCapabilityError",
    "ContentNotFoundError",
    "SnapshotNotFoundError",
    "TerminalStateViolationError",
    "IdempotencyConflictError",
    "InvalidQueryError",
]