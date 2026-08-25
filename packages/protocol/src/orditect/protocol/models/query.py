"""Query parameter models for contract read interfaces.

Iron rule: only mechanism fields (time range, pagination, ordering) are
modeled here. Business-predicate query DSLs (WHERE-style filters on business
fields) are explicitly out of scope for the protocol layer.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum

from pydantic import Field

from orditect.protocol.models._base import ContractModel


class SortDirection(str, Enum):
    """Sort direction for query results."""

    ASC = "asc"
    DESC = "desc"


class Page(ContractModel):
    """Pagination parameters.

    Attributes:
        limit: Maximum number of records to return (must be > 0).
        offset: Number of records to skip (>= 0).
    """

    limit: int = Field(default=100, gt=0)
    offset: int = Field(default=0, ge=0)


class Sort(ContractModel):
    """Ordering parameters.

    Attributes:
        field: Mechanism field to sort by (e.g. "created_at", "expire_at").
        direction: asc / desc.
    """

    field: str = "created_at"
    direction: SortDirection = SortDirection.DESC


class TimeRange(ContractModel):
    """Absolute time range filter (mechanism field).

    Attributes:
        start: Inclusive range start (None = unbounded).
        end: Exclusive range end (None = unbounded).
    """

    start: datetime | None = None
    end: datetime | None = None