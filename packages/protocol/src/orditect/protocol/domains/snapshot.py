"""Snapshot domain: execution snapshot persistence + tree/version queries.

This domain is the data foundation for resume / rerun / lineage DAG /
visualization / interruption. It is the warm-path projection of the lineage
semantics: the governance hot path (cascade cancel, resource-ledger
exemption) lives in the core engine's own store; this domain serves
observation and recovery (one semantics, two projections).

Key terms enforced here:
- T3  (terminal irreversibility): within one execution generation, a state
  declared terminal by the caller rejects further state mutation; a new
  generation (new execution_id) is always permitted.
- T4  (idempotency): (task_id, step, execution_id) is the save idempotency key.
- T11 (execution identity alignment): execution_id semantics are shared with
  the core hot record and the flow execution; divergence is a violation.
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from orditect.protocol.capabilities import CapabilitySet
from orditect.protocol.models import Page, Sort, TaskSnapshot, TimeRange


@runtime_checkable
class SnapshotWriter(Protocol):
    """Write side of the snapshot domain.

    Capability half-domain: snapshot_sink.
    """

    @property
    def capabilities(self) -> CapabilitySet:
        """Declared capability set (term T8)."""
        ...

    async def save(self, snapshot: TaskSnapshot) -> None:
        """Save one execution snapshot.

        Semantics: upserts the snapshot identified by
        (task_id, step, execution_id).

        Merge rule (T3 second face): when a record already exists for the
        same generation, non-state fields (parent_task_id, input_pointer,
        output_pointer, error, cost, model, expire_at) are merged to
        complete the record — fields present in the incoming snapshot
        overwrite, absent fields are preserved. `status` is NEVER merged
        (it is state); `updated_at` always advances to the latest write.

        Idempotency / concurrency (terms T4, T10): re-saving the same key
        with identical business content is a silent success. Concurrent
        saves with the same key must leave exactly one fully-written record;
        a partially written state must never be observable.

        Terminal-state rule (term T3): `terminal` states are declared by the
        caller via `save_terminal`; within one execution generation, after a
        terminal state has been recorded, further state mutations for that
        generation raise TerminalStateViolationError. Saving a snapshot with
        a *different* execution_id for the same task_id+step is always
        permitted (a new generation, term T11).

        Raises:
            TerminalStateViolationError: state mutation within a generation
                already recorded as terminal (T3).
            UnsupportedCapabilityError: snapshot_sink not declared (T8).
            ContractError: any other failure (T9).
        """
        ...

    async def save_terminal(self, snapshot: TaskSnapshot) -> None:
        """Save a snapshot whose state the caller declares terminal.

        Semantics: identical to `save`, plus marks this generation's state as
        terminal from this point on (term T3). Idempotent for the same key
        with an identical snapshot (T4).

        Raises:
            TerminalStateViolationError: this generation was already terminal
                and the incoming snapshot differs (T3).
            UnsupportedCapabilityError: snapshot_sink not declared (T8).
            ContractError: any other failure (T9).
        """
        ...


@runtime_checkable
class SnapshotReader(Protocol):
    """Read side of the snapshot domain.

    Capability half-domain: snapshot_query.

    All queries filter out expired snapshots lazily (expire_at, terms T1/T7)
    and treat status as an opaque string (term T6). Tree traversal must be
    cycle-safe and depth-bounded (contract terms, not implementation
    details): a lineage cycle must terminate traversal explicitly rather
    than loop, and depth beyond the caller's max_depth is truncated.
    `time_range` filters apply to `created_at` in every snapshot query.
    """

    @property
    def capabilities(self) -> CapabilitySet:
        """Declared capability set (term T8)."""
        ...

    async def get(
        self,
        task_id: str,
        step: str,
        *,
        execution_id: str | None = None,
    ) -> TaskSnapshot | None:
        """Read one snapshot (resume point lookup).

        Semantics: when execution_id is None, returns the latest generation's
        snapshot for (task_id, step); when given, returns that exact
        generation. Expired snapshots are invisible (T1).

        Raises:
            UnsupportedCapabilityError: snapshot_query not declared (T8).
            ContractError: any other failure (T9).
        """
        ...

    async def list_versions(
        self,
        task_id: str,
        step: str,
        *,
        page: Page | None = None,
        sort: Sort | None = None,
    ) -> list[TaskSnapshot]:
        """List execution versions of one node (time-travel / pick a version).

        Semantics: returns snapshots for (task_id, step) across all
        execution_ids, newest first by default (see Sort default). Expired
        snapshots are invisible (T1).

        Raises:
            UnsupportedCapabilityError: snapshot_query not declared (T8).
            ContractError: any other failure (T9).
        """
        ...

    async def get_tree(
        self,
        root_task_id: str,
        *,
        max_depth: int | None = None,
        latest_only: bool = True,
    ) -> list[TaskSnapshot]:
        """Return the full task subtree rooted at root_task_id (flat list).

        Semantics: returns root plus all descendant snapshots (the flat list
        is reassembled into a tree by the caller via parent_task_id). With
        latest_only=True (default), only the latest generation of each node
        is returned; False returns all generations (full time-travel view).

        Contract terms for traversal: cycle-safe (a parent_task_id cycle must
        terminate traversal, not loop) and depth-bounded (max_depth truncates;
        None means unbounded). Expired snapshots are invisible (T1).

        Raises:
            UnsupportedCapabilityError: snapshot_query not declared (T8).
            ContractError: any other failure (T9).
        """
        ...

    async def list_children(
        self,
        parent_task_id: str,
        *,
        latest_only: bool = True,
    ) -> list[TaskSnapshot]:
        """List direct children of one node (one level only).

        Semantics: single-level descendant listing for local DAG rendering
        and cascade analysis. Expired snapshots are invisible (T1).

        Raises:
            UnsupportedCapabilityError: snapshot_query not declared (T8).
            ContractError: any other failure (T9).
        """
        ...

    async def get_ancestors(
        self,
        task_id: str,
        *,
        include_self: bool = False,
    ) -> list[TaskSnapshot]:
        """List the ancestor chain of one node, root first.

        Semantics: walks parent_task_id upward. Cycle-safe (a cycle
        terminates the walk explicitly). Expired snapshots are invisible (T1).
        Returns the latest generation of each ancestor.

        Raises:
            UnsupportedCapabilityError: snapshot_query not declared (T8).
            ContractError: any other failure (T9).
        """
        ...

    async def query(
        self,
        *,
        status: str | None = None,
        parent_task_id: str | None = None,
        time_range: TimeRange | None = None,
        page: Page | None = None,
        sort: Sort | None = None,
    ) -> list[TaskSnapshot]:
        """Query snapshots by mechanism fields (running-set / filtered views).

        Semantics: AND-combination of provided filters. `status` is an opaque
        string (T6): filtering by a caller-declared non-terminal word yields
        the running set for interruption. Payload/input/output content
        filtering is out of contract scope (iron rule). Expired snapshots
        are invisible (T1). Returns latest generations only.
        `time_range` applies to `created_at`. `sort.field` must be within the
        contract mechanism whitelist (see mechanism.SORT_FIELDS).

        Raises:
            InvalidQueryError: sort.field outside the mechanism whitelist.
            UnsupportedCapabilityError: snapshot_query not declared (T8).
            ContractError: any other failure (T9).
        """
        ...

    async def aggregate(
        self,
        *,
        group_by: str,
        parent_task_id: str | None = None,
        time_range: TimeRange | None = None,
    ) -> dict[str, Any]:
        """Aggregate snapshots by a mechanism field (dashboard rollups).

        Semantics: groups latest-generation snapshots by `group_by`
        (a mechanism field from the contract whitelist, e.g. "status" or
        "model") and returns a mapping of group value ->
        {"count": int, "cost": dict[str, float]} where cost is summed per key
        across the group's snapshots. Business-metric aggregation beyond
        count/cost summation is out of contract scope.

        Precision note (Appendix C): cost summation is IEEE 754 double-
        precision accumulation with a committed relative error bound of 1e-9.
        Aggregated values are for display and monitoring only — never use
        them for reconciliation or billing decisions; reconcile via the
        audit domain instead.

        Raises:
            InvalidQueryError: group_by outside the mechanism whitelist.
            UnsupportedCapabilityError: snapshot_query not declared (T8).
            ContractError: any other failure (T9).
        """
        ...