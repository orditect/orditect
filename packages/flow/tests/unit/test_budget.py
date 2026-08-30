"""专题 3 钉扎：跨层结算协议（L2 预算耗尽拦截 L1 调用）。

递归愿景验收 #3：父任务预算账本约束子任务的受治理调用，
耗尽即拦截 BudgetExhaustedError，审计双写（sink 注入）正确。
"""
import asyncio
from typing import Any, Dict, List, Optional

import pytest

from orditect.flow import GovernedClient
from orditect.flow.governor.budget import (
    BudgetExhaustedError,
    BudgetLedger,
    NullAuditSink,
)


# ---------- test infrastructure ----------

class FakeQuotaDB:
    """内存版 AdmissionQuotaRedisDB（协议对齐）。"""

    def __init__(self):
        self._pending: Dict[str, int] = {}
        self._reserved: Dict[str, Dict[str, int]] = {}  # scope -> {task_id: units}

    async def reserve_units(self, *, scope, task_id, units, max_units, task_ttl_sec):
        if task_id in self._reserved.get(scope, {}):
            return {"ok": True, "reason": "already_reserved"}
        current = self._pending.get(scope, 0)
        if current + units > max_units:
            return {"ok": False, "reason": "limit_exceeded"}
        self._pending[scope] = current + units
        self._reserved.setdefault(scope, {})[task_id] = units
        return {"ok": True, "current": self._pending[scope]}

    async def get_pending_units(self, *, scope):
        return self._pending.get(scope, 0)


class SpyGovernor:
    async def acquire(self, resource: str, timeout: Optional[float] = None) -> str:
        return f"spy-{resource}"

    async def try_acquire(self, resource):
        return "spy"

    async def release(self, resource: str, token: str) -> None:
        pass

    async def get_usage(self, resource: str) -> int:
        return 0


class SpyAuditSink:
    """记录审计明细的 sink（选项 B 验证）。"""

    def __init__(self):
        self.records: List[Dict[str, Any]] = []

    async def record_charge(self, *, scope, call_id, units, balance_after):
        self.records.append({
            "scope": scope, "call_id": call_id,
            "units": units, "balance_after": balance_after,
        })


# ---------- test cases ----------

class TestBudgetLedgerBasics:
    async def test_open_and_balance(self):
        """开设账本后余额为 max_units。"""
        ledger = BudgetLedger(FakeQuotaDB(), root_task_id="root-1", max_units=1000)
        await ledger.open()
        assert await ledger.balance() == 1000

    async def test_charge_deducts_balance(self):
        """结算后余额正确扣减。"""
        ledger = BudgetLedger(FakeQuotaDB(), root_task_id="root-1", max_units=1000)
        await ledger.open()

        balance = await ledger.charge(300, call_id="call-1")
        assert balance == 700

    async def test_charge_idempotent_by_call_id(self):
        """同一 call_id 重试不重复扣费（quota 幂等）。"""
        ledger = BudgetLedger(FakeQuotaDB(), root_task_id="root-1", max_units=1000)
        await ledger.open()

        await ledger.charge(300, call_id="call-1")
        await ledger.charge(300, call_id="call-1")  # 重复
        assert await ledger.balance() == 700  # 只扣一次

    async def test_check_blocks_when_exhausted(self):
        """余额耗尽时 check 抛 BudgetExhaustedError。"""
        ledger = BudgetLedger(FakeQuotaDB(), root_task_id="root-1", max_units=100)
        await ledger.open()

        await ledger.charge(100, call_id="call-1")
        assert await ledger.balance() == 0

        with pytest.raises(BudgetExhaustedError, match="budget exhausted"):
            await ledger.check()

    async def test_overspend_allowed_but_recorded(self):
        """最后一笔允许超支（余额为负如实记录），但后续 check 拦截。"""
        ledger = BudgetLedger(FakeQuotaDB(), root_task_id="root-1", max_units=100)
        await ledger.open()

        # overspend 50 (balance -50)
        balance = await ledger.charge(150, call_id="call-1")
        assert balance == -50

        # subsequent calls intercepted
        with pytest.raises(BudgetExhaustedError):
            await ledger.check()


class TestGovernedClientBudget:
    """GovernedClient 预算集成（L2 预算拦截 L1 调用的验收）。"""

    async def test_call_charges_budget(self):
        """调用后按 cost_fn 计价结算到父账本。"""
        ledger = BudgetLedger(FakeQuotaDB(), root_task_id="root-1", max_units=1000)
        await ledger.open()

        async def llm_handler(prompt: str):
            return {"text": "response", "usage": {"total_tokens": 150}}

        client = GovernedClient(
            SpyGovernor(), "llm", handler=llm_handler,
            budget=ledger,
            cost_fn=lambda r: r["usage"]["total_tokens"],
        )

        await client.call("hello")
        assert await ledger.balance() == 850  # 1000 - 150

    async def test_exhausted_budget_blocks_call(self):
        """预算耗尽：L1 调用被拦截（BudgetExhaustedError），handler 未执行。"""
        ledger = BudgetLedger(FakeQuotaDB(), root_task_id="root-1", max_units=100)
        await ledger.open()
        await ledger.charge(100, call_id="seed")  # 预耗尽

        executed = []

        async def handler():
            executed.append(True)
            return {"usage": {"total_tokens": 10}}

        client = GovernedClient(
            SpyGovernor(), "llm", handler=handler,
            budget=ledger, cost_fn=lambda r: r["usage"]["total_tokens"],
        )

        with pytest.raises(BudgetExhaustedError):
            await client.call()

        assert executed == []  # handler 未执行（调用前拦截）

    async def test_no_budget_no_charge(self):
        """无 budget 时正常调用不结算（回归）。"""
        async def handler():
            return {"ok": True}

        client = GovernedClient(SpyGovernor(), "llm", handler=handler)
        result = await client.call()
        assert result == {"ok": True}

    async def test_default_cost_fn_counts_one(self):
        """默认 cost_fn：每次调用计 1 单位。"""
        ledger = BudgetLedger(FakeQuotaDB(), root_task_id="root-1", max_units=10)
        await ledger.open()

        async def handler():
            return "ok"

        client = GovernedClient(
            SpyGovernor(), "llm", handler=handler, budget=ledger,
        )

        await client.call()
        await client.call()
        assert await ledger.balance() == 8  # 10 - 2


class TestAuditSink:
    """审计双写（选项 B 预留验证）。"""

    async def test_audit_sink_receives_charge_records(self):
        """charge 时审计 sink 收到明细。"""
        sink = SpyAuditSink()
        ledger = BudgetLedger(
            FakeQuotaDB(), root_task_id="root-1", max_units=1000,
            audit_sink=sink,
        )
        await ledger.open()

        await ledger.charge(150, call_id="call-1")

        assert len(sink.records) == 1
        rec = sink.records[0]
        assert rec["scope"] == "budget:root-1"
        assert rec["units"] == 150
        assert rec["balance_after"] == 850

    async def test_audit_failure_does_not_block_settlement(self):
        """审计 sink 失败不阻塞结算（try/except 包裹）。"""
        class FailingSink:
            async def record_charge(self, **kwargs):
                raise RuntimeError("PG down")

        ledger = BudgetLedger(
            FakeQuotaDB(), root_task_id="root-1", max_units=1000,
            audit_sink=FailingSink(),
        )
        await ledger.open()

        # sink blows up, but settlement completes normally
        balance = await ledger.charge(100, call_id="call-1")
        assert balance == 900

    async def test_null_audit_sink_default(self):
        """默认 NullAuditSink：不报错，无明细。"""
        ledger = BudgetLedger(FakeQuotaDB(), root_task_id="root-1", max_units=100)
        await ledger.open()
        balance = await ledger.charge(50, call_id="c1")
        assert balance == 50  # 正常工作，无审计副作用

class TestChargeRejectedQuota:
    async def test_rejected_quota_write_is_logged_and_audited(self, caplog):
        """v0.1.6 pinning: a rejected quota write must surface as an error
        log instead of being silently swallowed; the audit record is still
        written (observation discipline), and the returned balance reflects
        the counter.

        Red before: reserve_units' ok flag was ignored — a quota rejection
        was invisible, while the audit record claimed the charge happened.
        """
        import logging

        class RejectingQuota(FakeQuotaDB):
            async def reserve_units(self, **kw):
                # Only actual charges (units > 0) are rejected; the ledger
                # open (units == 0) must succeed so the test reaches charge().
                if kw.get("units", 0) > 0:
                    return {"ok": False, "reason": "limit_exceeded"}
                return await super().reserve_units(**kw)

        sink = SpyAuditSink()
        ledger = BudgetLedger(
            RejectingQuota(), root_task_id="root-rej", max_units=100,
            audit_sink=sink,
        )
        await ledger.open()

        with caplog.at_level(logging.ERROR):
            balance = await ledger.charge(10, call_id="c-rej")

        assert any(
            "quota write rejected" in r.message for r in caplog.records
        ), "a rejected quota write must be logged explicitly"
        # audit record still written (the charge was real work; the ledger
        # divergence is now observable via the log + audit pair)
        assert len(sink.records) == 1
        assert sink.records[0]["units"] == 10
        # balance reflects the counter (nothing was deducted)
        assert balance == 100
