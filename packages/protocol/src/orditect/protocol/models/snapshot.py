"""TaskSnapshot: one execution record of one task node.

The snapshot is the data foundation for resume / replay / lineage DAG /
visualization. Key semantics:
- execution_id distinguishes multiple executions of the same task
  (resume / rerun produce a new execution_id; history is preserved).
- parent_task_id expresses the recursive nesting lineage.
- status is an opaque string (vocabulary neutrality: the protocol does not
  embed any business status words).
- input/output are pointer-ized (TaskPointer), never inline payloads.
- expire_at is an absolute expiry instant (lease term); readers filter on it.
"""

from __future__ import annotations
from datetime import UTC, datetime
from pydantic import Field
from orditect.protocol.models._base import ContractModel
from orditect.protocol.models.pointer import TaskPointer

def _utc_now() -> datetime:
    """Current time as timezone-aware UTC (single clock discipline)."""
    return datetime.now(UTC)


class TaskSnapshot(ContractModel):
    """Execution snapshot of a single task node.

    Attributes:
        task_id: Task identifier (deterministic-ID convention may apply).
        step: Step name within the task (opaque).
        execution_id: Execution generation identifier. A new execution of the
            same task (resume / rerun) uses a new execution_id, so multiple
            versions of the same node coexist for time-travel queries.
        parent_task_id: Parent task in the recursive lineage (None = root).
        status: Opaque status string (protocol embeds no vocabulary).
        input_pointer / output_pointer: Pointer-ized input/output content.
        error: Error message if the execution failed (opaque text).
        cost: Free-form cost metrics (token usage, USD, ...; business-defined).
        model: Optional model identifier (business-defined).
        created_at / updated_at: Record timestamps.
        expire_at: Absolute expiry instant for the lease term (None = no expiry).
    """
    task_id: str
    step: str
    execution_id: str
    parent_task_id: str | None = None
    status: str = ""
    input_pointer: TaskPointer | None = None
    output_pointer: TaskPointer | None = None
    error: str | None = None
    cost: dict[str, float] | None = None
    model: str | None = None
    created_at: datetime = Field(default_factory=_utc_now)
    updated_at: datetime = Field(default_factory=_utc_now)
    expire_at: datetime | None = None