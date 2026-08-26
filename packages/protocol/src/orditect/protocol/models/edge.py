"""DependencyEdge / DependencyGraph: dependency-graph domain models (T12).

Design discipline:
- Edges are fact records; the store makes no graph-theoretic judgement —
  a self-loop or mutual cycle in the data is NOT rejected here (cycle
  detection is not the store's job, T12: prevention lives with the
  registrar, discovery with offline tools).
- Nodes are task_id references only — never carry properties (prevents
  dual-write drift between this domain and the snapshot domain).
- An edge binds the task, not an execution generation: reopen (a new
  execution_id) never rewrites edges.
"""

from __future__ import annotations

from datetime import UTC, datetime

from pydantic import Field

from orditect.protocol.models._base import ContractModel


def _utc_now() -> datetime:
    """Current time as timezone-aware UTC (single clock discipline, T7)."""
    return datetime.now(UTC)


class DependencyEdge(ContractModel):
    """One directed dependency fact: child depends on parent.

    Idempotency key = (child_id, parent_id): re-writing with an identical
    payload is a silent dedup; with a different is_primary it is a conflict
    (IdempotencyConflictError, T4).

    Attributes:
        child_id: Dependent task id (opaque reference, T6).
        parent_id: Dependency task id. Binds the task, not an execution
            generation (T12).
        is_primary: Primary-parent flag — the single chain used for lineage
            and exemption inheritance.
        registered_at: Edge registration instant, timezone-aware UTC (T7).
    """

    child_id: str
    parent_id: str
    is_primary: bool = False
    registered_at: datetime = Field(default_factory=_utc_now)


class DependencyGraph(ContractModel):
    """Dependency neighbourhood returned by read_graph.

    The transitive closure reachable from root_task_id along edges in BOTH
    directions (upstream "what I depend on" + downstream "who depends on
    me"), cycle-safe by construction of the reader. task_ids are identifiers
    only — no node properties (pure-edge discipline, T12). root_task_id is
    always present in task_ids, even when it has no edges.

    Attributes:
        root_task_id: The root this neighbourhood was read from.
        task_ids: All reachable task identifiers (sorted for determinism).
        edges: All edges whose child_id is within the closure.
    """

    root_task_id: str
    task_ids: list[str] = Field(default_factory=list)
    edges: list[DependencyEdge] = Field(default_factory=list)