"""Framework-neutral exceptions.
Discipline: framework is agnostic to HTTP / BizError; app side maps to business
exceptions with code/http_status.
"""
from __future__ import annotations

from typing import Literal


class TaskbaseError(Exception):
    """Base class for all framework exceptions."""


class AcquireTimeoutError(TaskbaseError, TimeoutError):
    """acquire(timeout) wait exceeded.

    Also inherits built-in TimeoutError: app legacy code `except TimeoutError`
    needs no changes.
    """


class LimiterUnavailableError(TaskbaseError):
    """Redis backend persistently unavailable and policy is fail-close
    (strategy enumeration explicit).

    Implementation trigger condition: before production (see
    docs/design_decisions.md).
    """


class TaskNotFoundError(TaskbaseError):
    """task_id does not exist."""


class InvalidStatusTransferError(TaskbaseError):
    """Illegal status transfer (including terminal state overwritten)."""


class CancelledByUser(TaskbaseError):
    """Task internal active interruption (CancellationToken.raise_if_cancelled)."""


class InvalidUsageError(TaskbaseError):
    """API usage error.

    Typical scenario: calling self-managed-mode-only methods (e.g. close())
    in dependency injection mode. Contract explicitness — misuse immediately
    raises error, not silently skipped.
    """


#: W4 decision implementation (interface surface): explicit strategy enum
#: when Redis is unavailable. Implementation (fail-open local degraded
#: permission) completed before production, currently only type contract defined.
UnavailablePolicy = Literal["fail_open", "fail_close"]