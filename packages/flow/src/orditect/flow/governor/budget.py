"""Cross-layer settlement protocol (Topic 3): parent task budget constrains child task calls.

Core semantics:
- Parent task (L2) creates a budget ledger (scope=budget:{root_task_id})
- Each governed call from a child task (L1) is settled against the parent ledger (post-charge)
- Pre-check balance (>0) before calling; intercept with BudgetExhaustedError when exhausted

Ledger carrier: taskbase AdmissionQuotaRedisDB (ZSET lease + atomic pre-reservation +
crash recovery, zero taskbase changes).

Cost model: post-charge (settled after call based on actual consumption) — LLM tokens are
posterior, cannot be predicted before calling. Pre-check only checks "balance > 0", no estimation.

Audit reservation (Option B): dual-write audit_sink on charge (injectable),
switch to PostgresAuditSink when taskstore (PG audit details) is ready.
Current default is NullAuditSink (only Redis real-time balance, no persisted details).

Changes:
- open() consumes taskbase's units=0 ledger-creation semantics and checks the return value —
  fixes silent failure (units=0 rejected by Lua invalid_units without awareness).
- charge() call_id semantics upgraded to "dual-purpose idempotency key":
  hot path (quota already_reserved deduplication) and cold path (taskstore audit table
  call_id PK ON CONFLICT DO NOTHING) share the same key — when a business retries the same
  logical call with the same call_id, both layers deduplicate: no double charge, no double audit.

Usage example:
    # L2 parent task creates budget
    ledger = BudgetLedger(quota_db, root_task_id=task_id, max_units=1000)
    await ledger.open()

    # L1 child task calls via GovernedClient (auto-settlement)
    llm = GovernedClient(governor, resource="llm", handler=call_llm,
                         budget=ledger, cost_fn=lambda r: r["usage"]["total_tokens"])
    result = await llm.call(prompt)  # settled to parent ledger after call based on usage
"""
from __future__ import annotations

import logging
from typing import Any, Awaitable, Callable, Optional, Protocol

from orditect.flow.exceptions import TaskflowError

logger = logging.getLogger(__name__)


class BudgetExhaustedError(TaskflowError):
    """Budget exhausted: parent task ledger balance insufficient, intercepts child task calls."""


# : pricing function signature: handler return value → cost (units, business-defined semantics)
CostFn = Callable[[Any], int]


class BudgetAuditSink(Protocol):
    """Audit detail write protocol (Option B: reserved interface before taskstore is ready).

        Implementations: taskstore's PostgresAuditSink (future) / custom business sinks.
        Calls are wrapped in try/except — audit failure must never block business settlement.

        Idempotency discipline (frozen): implementations must deduplicate using call_id as the unique key
        (PG: ON CONFLICT DO NOTHING). The framework will still call this protocol even when quota returns
        already_reserved — cold-path deduplication is the implementer's responsibility.
        """

    async def record_charge(
        self,
        *,
        scope: str,
        call_id: str,
        units: int,
        balance_after: int,
    ) -> None:
        """Record a settlement detail.

                Args:
                    scope: Ledger scope (budget:{root_task_id})
                    call_id: Unique identifier for this call (dual-purpose idempotency key, see BudgetLedger.charge)
                    units: Settlement cost
                    balance_after: Balance after settlement
                """
        ...


class NullAuditSink:
    """Null audit implementation (default): no persisted details, only Redis real-time balance."""

    async def record_charge(self, **kwargs) -> None:
        pass


class BudgetLedger:
    """Budget ledger (created by parent task, settled by child tasks).

        Semantics:
        - open(): opens ledger with max_units (idempotent, retry does not reset budget)
        - check(): pre-call check (balance > 0 allows passage)
        - charge(): post-call settlement (deducts actual cost, can be negative — overspend recorded as-is)
        - balance(): queries current balance
        """

    def __init__(
        self,
        quota_db: Any,  # AdmissionQuotaRedisDB（鸭子类型）
        *,
        root_task_id: str,
        max_units: int,
        task_ttl_sec: int = 86400,
        audit_sink: Any = None,  # BudgetAuditSink 实现（None=NullAuditSink）
    ):
        """
                Args:
                    quota_db: taskbase AdmissionQuotaRedisDB instance (requires >= 0.3.2,
                        open() relies on its units=0 ledger-creation semantics)
                    root_task_id: root task ID of the task tree (component of the ledger scope)
                    max_units: budget upper limit (units, semantics defined by business: token count/cents/call count)
                    task_ttl_sec: ledger TTL (default 1 day; ledger auto-reclaimed after task tree crash)
                    audit_sink: audit detail write implementation (Option B reserved, default NullAuditSink)
                """
        self._quota = quota_db
        self._root_task_id = root_task_id
        self._max_units = int(max_units)
        self._ttl = int(task_ttl_sec)
        self._audit = audit_sink or NullAuditSink()

    @property
    def scope(self) -> str:
        return f"budget:{self._root_task_id}"

    @property
    def root_task_id(self) -> str:
        return self._root_task_id

    async def open(self) -> None:
        """Open the ledger (initialize balance to max_units, idempotent).

                Consumes taskbase quota_reserve's units=0 ledger-creation semantics
                (registers lease slot but does not consume quota), and checks the return value —
                no longer fails silently.

                Raises:
                    TaskflowError: ledger creation rejected by underlying quota storage
                """
        result = await self._quota.reserve_units(
            scope=self.scope,
            task_id="__ledger__",
            units=0,  # 建账语义：登记租约位但不消耗额度（taskbase >= 0.3.2）
            max_units=self._max_units,
            task_ttl_sec=self._ttl,
        )
        if not result.get("ok"):
            raise TaskflowError(
                f"budget ledger open failed: scope={self.scope} "
                f"reason={result.get('reason')}"
            )
        logger.info(
            f"Budget ledger opened: scope={self.scope} max_units={self._max_units}"
        )

    async def balance(self) -> int:
        """Query current balance (max_units - consumed)."""
        used = await self._quota.get_pending_units(scope=self.scope)
        return self._max_units - used

    async def check(self) -> None:
        """Pre-call check: raise BudgetExhaustedError if balance <= 0 (interception).

                Note: only checks "balance > 0", no estimation (estimation is meaningless under the post-charge model).
                """
        balance = await self.balance()
        if balance <= 0:
            raise BudgetExhaustedError(
                f"budget exhausted: scope={self.scope} "
                f"max_units={self._max_units} balance={balance}"
            )

    async def charge(self, units: int, *, call_id: str) -> int:
        """Post-call settlement: deduct actual cost + audit dual-write (Option B).

        Args:
            units: actual cost (>0; if <=0, returns current balance directly)
            call_id: dual-purpose idempotency key (semantics frozen):
                - hot path: quota already_reserved deduplication (retry does
                  not double charge)
                - cold path: taskstore audit table call_id PK ON CONFLICT
                  (retry does not double audit)
                Business retries of the same logical call must use the same
                call_id; framework default is "call-{uuid}" (one new audit
                record per call).

        Returns:
            post-settlement balance (may be negative, overspend recorded
            as-is). v0.1.6: when the quota write is rejected (e.g. extreme
            overspend past the 1000x headroom), the failure is logged
            explicitly — the audit record is still written (observation
            discipline), but callers can now tell the two ledgers diverged.
        """
        if units <= 0:
            return await self.balance()

        # 1. Redis real-time deduction (only basis for interception decision)
        result = await self._quota.reserve_units(
            scope=self.scope,
            task_id=call_id,
            units=units,
            # allow overspend: max_units enlarged, quota does not intercept
            # (interception decision in check(), charge only honestly records)
            max_units=self._max_units * 1000,
            task_ttl_sec=self._ttl,
        )
        if not result.get("ok"):
            # v0.1.6: never silently swallow a rejected quota write — the
            # audit trail below and the Redis counter would diverge.
            logger.error(
                f"Budget charge quota write rejected: scope={self.scope} "
                f"call_id={call_id} units={units} "
                f"reason={result.get('reason')} "
                f"(audit record will still be written; ledgers may diverge)"
            )
        balance = await self.balance()

        # 2. audit detail dual-write (option B reserved; try/except wrapped,
        # never blocks business. dedup on retry same call_id cold path
        # handled by sink's PK constraint)
        try:
            await self._audit.record_charge(
                scope=self.scope,
                call_id=call_id,
                units=units,
                balance_after=balance,
            )
        except Exception as e:
            logger.warning(f"Budget audit sink failed (settlement unaffected): {e}")

        logger.debug(
            f"Budget charged: scope={self.scope} call_id={call_id} "
            f"units={units} balance={balance}"
        )
        return balance