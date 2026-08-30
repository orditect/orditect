"""Global governance client: call-point level resource governance (global layer of dual-layer governance).

Dual-layer governance architecture:
- Task governance: task boundary management (task.resource_type), acquire on task start, release on task end
  (handled by TaskExecutor)
- Global governance: call-point management (GovernedClient), acquire on call start, release on call end
  (handled by this class)

A long-running task typically has multiple call points (DB query, LLM call, OSS upload).
Task governance holds task-level concurrency slots, while global governance holds concurrency slots
for specific downstream resources. They work independently and in parallel.

Usage example:
    class AgentTask(BaseBackEndTask):
        resource_type = "task_agent"  # task governance type

        async def execute(self, task_id: str, **kwargs):
            governor = kwargs.get("governor")  # injected by TaskExecutor

            llm = GovernedClient(governor, resource="default_stream_llm",
                                 handler=call_llm)
            oss = GovernedClient(governor, resource="oss_upload",
                                 handler=upload_to_oss)

            # Node 1: DB query (no global governance)
            data = await db.query(...)

            # Node 2: batch LLM calls (global governance)
            results = await asyncio.gather(*[llm.call(item) for item in data])

            # Node 3: OSS upload (global governance)
            await oss.call(results)

Cancellation semantics:
    When call(cancel_token=...) is passed a cancellation token:
    - If cancelled before call: skip acquire, do not execute, return None
    - If cancelled after acquire: do not execute, return None (token released normally in finally)
    Returning None rather than raising an exception facilitates partial cancellation in asyncio.gather.

Budget semantics (Topic 3):
    When call(budget is injected):
    - Budget pre-check occurs before acquire (#23: do not queue for downstream resources when exhausted)
    - After call, cost is settled to the parent ledger via cost_fn (post-charge)
    - call_id is a dual-purpose idempotency key (see call() parameter documentation)
"""
from __future__ import annotations

import asyncio
import logging
import uuid
from typing import Any, Awaitable, Callable, Optional

from orditect.flow.protocols.governor import ResourceGovernorProtocol

logger = logging.getLogger(__name__)

# : callable object signature: async def handler(*args, **kwargs) -> Any
Handler = Callable[..., Awaitable[Any]]

def _retrieve_release_error(task: "asyncio.Task") -> None:
    """Retrieve exceptions from shielded release tasks (prevents
    'exception was never retrieved' warnings at GC time)."""
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        logger.debug(f"Governed release finished with error: {exc}")

class GovernedClient:
    """Global governance client (call-point level resource governance).

        Responsibilities:
        - Acquire resource token before call, release in finally (prevent leaks)
        - Support cancellation token (cancel_token): skip execution if cancelled
        - Support budget ledger (budget): pre-check interception + settlement
        - handler can be bound at construction (reuse the same call point) or passed at call time

        Args:
            governor: Resource governance instance (injected by TaskExecutor via kwargs)
            resource: Global governance resource name (e.g., "default_stream_llm", "oss_upload")
            handler: Default callable (optional, can be overridden at call time)
            timeout: Max wait seconds for acquire (default 30.0. Note: this is separate from
                TaskExecutor's acquire_timeout default None=infinite queueing — call-point governance
                is user-facing and has bounded wait; task-boundary governance defaults to infinite)
            budget: Budget ledger (BudgetLedger, Topic 3). When not None:
                check() pre-check before acquire, raise BudgetExhaustedError if exhausted;
                charge() after call with cost_fn to settle to parent ledger.
            cost_fn: Pricing function (handler return value → units).
                Recommended to provide explicitly when budget is not None; defaults to 1 unit per call.
        """

    def __init__(
        self,
        governor: ResourceGovernorProtocol,
        resource: str,
        handler: Optional[Handler] = None,
        *,
        timeout: float = 30.0,
        budget: Any = None,
        cost_fn: Optional[Callable] = None,
    ):
        if governor is None:
            raise ValueError("GovernedClient requires a governor (got None)")
        if not resource:
            raise ValueError("GovernedClient requires a non-empty resource name")

        self.governor = governor
        self.resource = resource
        self.handler = handler
        self.timeout = timeout
        self._budget = budget
        self._cost_fn = cost_fn or (lambda result: 1)
        # v0.1.6: strong refs for shielded release tasks (mirrors the
        # executor's _finalize_tasks discipline — an orphaned shield task
        # must never be GC-collected mid-release).
        self._release_tasks: set[asyncio.Task] = set()

    async def call(
        self,
        *args,
        cancel_token: Any = None,
        handler: Optional[Handler] = None,
        call_id: str | None = None,
        **kwargs,
    ) -> Any:
        """Governed call: cancel check → budget pre-check → acquire → execute → settle → finally release.

                Args:
                    *args: Positional arguments to pass to handler
                    cancel_token: Cancellation token (duck-typed: any object with async is_cancelled(),
                        e.g., orditect.core.CancellationToken).
                        Skip execution and return None if already cancelled.
                    handler: Function for this call (overrides the default handler bound at construction)
                    call_id: Dual-purpose idempotency key (for budget settlement):
                        - None: framework generates "call-{uuid}" (one new audit record per call, correct default)
                        - Explicit: business retries the same logical call with the same call_id,
                          hot path (quota already_reserved) and cold path (audit table call_id PK)
                          deduplicate at both layers — retries do not double charge or double audit.
                    **kwargs: Keyword arguments to pass to handler

                Returns:
                    Handler's return value; None if skipped due to cancellation

                Raises:
                    ValueError: No handler provided (neither at construction nor call time)
                    AcquireTimeoutError: Timeout acquiring resource token (raised by governor)
                    BudgetExhaustedError: Budget exhausted (when budget is injected, intercepted before acquire)
                """
        fn = handler or self.handler
        if fn is None:
            raise ValueError(
                f"GovernedClient.call requires a handler "
                f"(resource='{self.resource}'): "
                f"pass one at construction or call time"
            )

        # 1. pre-call cancellation check: if already cancelled, do not acquire (avoid wasting queue slot)
        if cancel_token is not None and await cancel_token.is_cancelled():
            logger.info(
                f"GovernedClient skipped before acquire (cancelled): "
                f"resource={self.resource}"
            )
            return None

        # 2. budget pre-check moved before acquire —
        # when budget exhausted, do not occupy downstream resource quota slot)
        if self._budget is not None:
            await self._budget.check()

        # 3. acquire resource token
        logger.debug(f"GovernedClient acquiring: resource={self.resource}")
        token = await self.governor.acquire(self.resource, timeout=self.timeout)

        try:
            # 4. post-acquire cancellation check: if cancelled, do not execute, but token must be released in finally
            if cancel_token is not None and await cancel_token.is_cancelled():
                logger.info(
                    f"GovernedClient skipped after acquire (cancelled): "
                    f"resource={self.resource}"
                )
                return None

            # 5. execute actual call
            result = await fn(*args, **kwargs)

            # 6. budget settlement (post-charge: deduct actual cost from parent ledger)
            if self._budget is not None:
                await self._budget.charge(
                    self._cost_fn(result),
                    call_id=call_id or f"call-{uuid.uuid4().hex[:12]}",
                )

            return result

        finally:
            # 7. release resource token (R12: shield prevents second
            #    cancellation swallowing release; the inner task is
            #    strong-referenced so it survives GC — v0.1.6)
            logger.debug(f"GovernedClient releasing: resource={self.resource}")
            await self._shielded_release(token)

    async def _shielded_release(self, token: str) -> None:
        """Release with shield + strong reference (mirrors executor
        _shielded_finalize), with a RuntimeError fallback for
        loop-teardown windows (v0.1.7): close the coroutine (no
        'never awaited' leak), log, and skip the release."""
        release_coro = self.governor.release(self.resource, token)
        try:
            task = asyncio.create_task(release_coro)
        except RuntimeError:
            release_coro.close()
            logger.warning(
                "release skipped: no running event loop (teardown phase)"
            )
            return
        self._release_tasks.add(task)
        task.add_done_callback(self._release_tasks.discard)
        task.add_done_callback(_retrieve_release_error)
        await asyncio.shield(task)