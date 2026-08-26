"""Dependency domain: append-only storage and graph queries for multi-parent
dependency edges.

Domain semantics (T12):
- Pure-edge model: this domain stores edges only; nodes are task_id
  references and never carry properties (no dual-write drift).
- An edge binds the task, not an execution generation: reopen (a new
  execution_id) never rewrites edges.
- Cycle detection is not the store's job: the writer records facts as given
  (self-loops and mutual cycles are data, not errors); cycle prevention
  lives with the registrar, discovery with offline tools.
- Batch atomicity across edges is not guaranteed: partial edge writes on
  failure are compensated by offline rebuild tools.

Profile mapping (conformance, M3): dependency_sink belongs to the producer
profile; dependency_query to the consumer profile.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from orditect.protocol.capabilities import CapabilitySet
from orditect.protocol.models import DependencyEdge, DependencyGraph


@runtime_checkable
class DependencyWriter(Protocol):
    """Write side of the dependency domain.

    Capability half-domain: dependency_sink.
    """

    @property
    def capabilities(self) -> CapabilitySet:
        """Declared capability set (term T8)."""
        ...

    async def write_dependency(self, edge: DependencyEdge) -> None:
        """Write one dependency edge (idempotent, T4).

        Idempotency key = (edge.child_id, edge.parent_id): re-writing with an
        identical is_primary is a silent dedup; re-writing with a different
        is_primary raises IdempotencyConflictError. The store makes no cycle
        judgement (T12).

        Raises:
            IdempotencyConflictError: same key, different is_primary (T4).
            UnsupportedCapabilityError: dependency_sink not declared (T8).
            ContractError: any other failure (T9).
        """
        ...


@runtime_checkable
class DependencyReader(Protocol):
    """Read side of the dependency domain.

    Capability half-domain: dependency_query.
    """

    @property
    def capabilities(self) -> CapabilitySet:
        """Declared capability set (term T8)."""
        ...

    async def read_graph(self, root_task_id: str) -> DependencyGraph:
        """Dependency neighbourhood: the transitive closure reachable from
        root_task_id along edges in BOTH directions (upstream dependencies +
        downstream dependents).

        Cycle-safe (visited-set termination, T12). Returns a graph whose
        task_ids always includes root_task_id, even when it has no edges.

        Raises:
            UnsupportedCapabilityError: dependency_query not declared (T8).
            ContractError: any other failure (T9).
        """
        ...

    async def all_edges(self) -> list[DependencyEdge]:
        """Enumerate every edge (data source for offline tools).

        Scale boundary: intended for offline scan / rebuild tools at up to
        ~100k edges; callers must not poll this on any hot path.

        Raises:
            UnsupportedCapabilityError: dependency_query not declared (T8).
            ContractError: any other failure (T9).
        """
        ...

    async def children_of(self, parent_task_id: str) -> list[str]:
        """One-level downstream: task_ids that directly depend on
        parent_task_id (no transitive expansion).

        Raises:
            UnsupportedCapabilityError: dependency_query not declared (T8).
            ContractError: any other failure (T9).
        """
        ...

    async def parents_of(self, child_task_id: str) -> list[str]:
        """One-level upstream: task_ids that child_task_id directly depends
        on (no transitive expansion).

        Raises:
            UnsupportedCapabilityError: dependency_query not declared (T8).
            ContractError: any other failure (T9).
        """
        ...