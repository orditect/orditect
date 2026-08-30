"""GovernedCallClient: the standard form of one governed call.

Composes the established governance half (GovernedClient: cancel pre-check ->
budget pre-check -> acquire -> execute -> charge -> shielded release) with an
observation half (audit record + content pointer-ization) and three opaque
labels (task_id / parent_task_id / execution_id).

Design discipline:
- Composition, not reinvention: the non-streaming path delegates to
  GovernedClient unchanged; this client only adds observation around it.
- Vocabulary neutrality: event_type / payload keys are caller-injected
  vocabulary; this module embeds none.
- Observation non-blocking (T9): audit and pointer writes are wrapped so a
  failing sink never disturbs the call path.
- Idempotency (T4): one call one call_id; the audit record uses
  event_id == call_id, so a retry with the same call_id dedups at the audit
  storage layer exactly like the quota layer dedups the charge.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from collections.abc import AsyncIterator, Callable
from typing import Any

from orditect.flow.governor.client import GovernedClient, Handler
from orditect.flow.protocols.governor import ResourceGovernorProtocol
from orditect.protocol import AuditEvent

logger = logging.getLogger(__name__)

def _retrieve_release_error(task: "asyncio.Task") -> None:
    """Retrieve exceptions from shielded release tasks (prevents
    'exception was never retrieved' warnings at GC time)."""
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        logger.debug(f"Governed call release finished with error: {exc}")

#: payload_fn: call result -> audit payload dict (caller-injected vocabulary).
PayloadFn = Callable[[Any], dict]
#: content_fn: call result -> bytes worth pointer-izing (None = nothing).
ContentFn = Callable[[Any], "bytes | None"]
#: result_fn: () -> final aggregated result of a stream (None when the source
#: never reports one, e.g. a stream without usage).
ResultFn = Callable[[], Any]
#: partial_fn: () -> partial bytes accumulated up to cancellation.
PartialFn = Callable[[], "bytes | None"]


class GovernedCallClient:
    """Standard form of one governed call (mechanism only, no business words).

    Args:
        governor: Resource governance instance.
        resource: Resource name for semaphore routing.
        handler: Default callable; may be overridden per call.
        timeout: Acquire wait bound in seconds.
        budget: BudgetLedger (optional). Pre-check before acquire; post-charge
            after success via cost_fn.
        cost_fn: result -> units for budget charging. Receives the raw result
            (call) or the aggregated stream result (call_streaming, possibly
            None when the source reports no usage — the business prices it).
        audit_writer: Protocol AuditWriter (optional). When set, exactly one
            AuditEvent is written per call at completion (ok / error /
            cancelled) with event_id == call_id.
        content_writer: Protocol ContentWriter (optional). When set and a
            content_fn / partial_fn yields bytes, those bytes are
            pointer-ized (T5) and the pointer is recorded in the audit
            payload under "pointer".
        event_type: Caller-declared audit event_type (caller vocabulary).
        task_id / parent_task_id / execution_id: Opaque labels (all optional).
            Absent labels degrade the client to pure call governance; present
            labels attach the call to the orchestration lineage.
        scope: Optional AuditEvent.scope passthrough.
        content_type: content_type recorded when pointer-izing bytes.

    Usage:
        client = GovernedCallClient(governor, "res", handler=do_work,
                                    audit_writer=audit, event_type="work_call",
                                    task_id=task_id)
        result = await client.call(call_id="c-1")
    """

    def __init__(
        self,
        governor: ResourceGovernorProtocol,
        resource: str,
        handler: Handler | None = None,
        *,
        timeout: float = 30.0,
        budget: Any = None,
        cost_fn: Callable | None = None,
        audit_writer: Any = None,
        content_writer: Any = None,
        event_type: str = "",
        task_id: str | None = None,
        parent_task_id: str | None = None,
        execution_id: str | None = None,
        scope: str | None = None,
        content_type: str | None = None,
    ):
        self.governor = governor
        self.resource = resource
        self.timeout = timeout
        self.handler = handler
        self._budget = budget
        self._cost_fn = cost_fn or (lambda result: 1)
        self._audit_writer = audit_writer
        self._content_writer = content_writer
        self._event_type = event_type
        self._task_id = task_id
        self._parent_task_id = parent_task_id
        self._execution_id = execution_id
        self._scope = scope
        self._content_type = content_type
        # Governance half, reused unchanged (its constructor validates
        # governor / resource for us).
        self._governed = GovernedClient(
            governor,
            resource,
            handler,
            timeout=timeout,
            budget=budget,
            cost_fn=self._cost_fn,
        )
        # v0.1.6: strong refs for shielded release tasks (mirrors
        # GovernedClient / executor discipline).
        self._release_tasks: set[asyncio.Task] = set()

    # ---------- non-streaming ----------

    async def call(
        self,
        *args,
        payload_fn: PayloadFn | None = None,
        content_fn: ContentFn | None = None,
        cancel_token: Any = None,
        handler: Handler | None = None,
        call_id: str | None = None,
        **kwargs,
    ) -> Any:
        """One governed non-streaming call with observation.

        Returns the handler result. Raises whatever governance / execution
        raised (budget exhaustion, acquire timeout, handler error); an audit
        record is written for every attempt that passed the budget pre-check.
        Calls blocked by budget exhaustion or cancelled before acquire leave
        no audit record.
        """
        fn = handler or self.handler
        if fn is None:
            raise ValueError(
                f"GovernedCallClient.call requires a handler "
                f"(resource={self.resource!r})"
            )
        cid = call_id or f"call-{uuid.uuid4().hex[:12]}"

        # Budget pre-check happens BEFORE entering the audited region: a
        # blocked attempt never reached the resource and must not produce an
        # audit record (T9 observation fidelity).
        if self._budget is not None:
            await self._budget.check()

        if cancel_token is not None and await cancel_token.is_cancelled():
            return None

        started = time.monotonic()
        executed = False

        async def wrapped(*a, **kw):
            nonlocal executed
            executed = True
            return await fn(*a, **kw)

        result: Any = None
        error: BaseException | None = None
        cancelled = False
        try:
            result = await self._governed.call(
                *args,
                cancel_token=cancel_token,
                handler=wrapped,
                call_id=cid,
                **kwargs,
            )
            # C5 (v0.1.5): GovernedClient returns None when the token flipped
            # to cancelled AFTER acquire (instead of raising) — mark the audit
            # record as cancelled instead of ok. (Approximation: a handler
            # legitimately returning None at the same instant is mislabeled;
            # documented as acceptable, it only affects the audit flag.)
            if (result is None and cancel_token is not None
                    and await cancel_token.is_cancelled()):
                cancelled = True
            return result
        except asyncio.CancelledError:
            cancelled = True
            raise
        except BaseException as e:
            error = e
            raise
        finally:
            # Note: only attempts that acquired the resource (executed) or
            # failed mid-execution are audited. Budget blocks and pre-acquire
            # cancellations are excluded by the early returns above.
            if executed or error is not None or cancelled:
                # C2 (v0.1.5): cost_fn is pure; its output feeds both budget
                # charging (inside GovernedClient) and observation. Record it
                # whenever it is computed for a successful call.
                cost_units: int | None = None
                if error is None and not cancelled and self._budget is not None:
                    try:
                        cost_units = self._cost_fn(result)
                    except Exception as e:
                        logger.warning(f"cost_fn failed (audit continues): {e}")
                await self._finalize(
                    cid,
                    ok=error is None and not cancelled,
                    result=result,
                    error=error,
                    cancelled=cancelled,
                    elapsed=time.monotonic() - started,
                    payload_fn=payload_fn,
                    content_fn=content_fn,
                    cost_units=cost_units,
                )

    # ---------- streaming ----------

    async def call_streaming(
            self,
            *args,
            handler: Callable[..., AsyncIterator] | None = None,
            payload_fn: PayloadFn | None = None,
            result_fn: ResultFn | None = None,
            partial_fn: PartialFn | None = None,
            cancel_token: Any = None,
            call_id: str | None = None,
            **kwargs,
    ) -> AsyncIterator[Any]:
        """One governed streaming call as an async generator.

        Semantics (streaming-governance spec):
        - The semaphore is held for the stream's whole lifetime and released
          (shielded) when the stream closes.
        - Exactly one audit event is written when the stream closes (ok /
          error / cancelled); deltas are output-plane traffic, never ledger
          rows.
        - cost_fn is evaluated at stream end over result_fn() (possibly None
          when the source reports no usage — the business prices it);
          charging happens only on normal completion, mirroring call().
        - On cancellation (caller break / aclose / task cancel), partial_fn()
          bytes are pointer-ized and the audit record is marked cancelled.
        - The handler's generator is deterministically closed (aclose
          cascade, v0.1.7): async-for never acloses inner iterators, so
          inner resources (e.g. an HTTP stream) would otherwise be left to
          GC timing.

        Note: as an async generator, validation and the governance prologue
        run on first iteration, not at call time.
        """
        fn = handler or self.handler
        if fn is None:
            raise ValueError(
                f"GovernedCallClient.call_streaming requires a handler "
                f"(resource={self.resource!r})"
            )
        cid = call_id or f"call-{uuid.uuid4().hex[:12]}"

        # Budget pre-check before acquire: blocked attempts leave no record.
        if self._budget is not None:
            await self._budget.check()

        if cancel_token is not None and await cancel_token.is_cancelled():
            return

        token = await self.governor.acquire(self.resource, timeout=self.timeout)
        started = time.monotonic()
        ok = False
        cancelled = False
        error: BaseException | None = None
        result: Any = None
        cost: int | None = None  # only computed on normal completion
        gen = None
        try:
            gen = fn(*args, **kwargs)
            async for chunk in gen:
                yield chunk
            if result_fn is not None:
                result = result_fn()
            # cost_fn is evaluated whenever a result exists (its output feeds
            # both budget charging AND observation) — it must NOT depend on
            # budget being present.
            cost = self._cost_fn(result)
            if self._budget is not None:
                await self._budget.charge(cost, call_id=cid)
            ok = True
        except GeneratorExit:
            # Consumer broke out early / aclose(): cancelled stream.
            cancelled = True
            raise
        except asyncio.CancelledError:
            cancelled = True
            raise
        except BaseException as e:
            error = e
            raise
        finally:
            # v0.1.7: deterministically close the handler's generator so its
            # finally chain (e.g. an httpx stream's connection cleanup) runs
            # now, not at GC time.
            if gen is not None:
                aclose = getattr(gen, "aclose", None)
                if aclose is not None:
                    try:
                        await aclose()
                    except Exception as e:
                        logger.debug(
                            f"handler stream aclose failed (ignored): {e}"
                        )
            pointer_data: bytes | None = None
            if cancelled and partial_fn is not None:
                try:
                    pointer_data = partial_fn()
                except Exception as e:
                    logger.warning(
                        f"partial_fn failed (audit continues): {e}"
                    )
            await self._finalize(
                cid,
                ok=ok,
                result=result,
                error=error,
                cancelled=cancelled,
                elapsed=time.monotonic() - started,
                payload_fn=payload_fn,
                pointer_data=pointer_data,
                cost_units=cost,
            )
            # v0.1.6: shield + strong reference for the release task, so an
            # orphaned shield task is never GC-collected mid-release (the
            # RuntimeError fallback still covers the teardown-phase case).
            try:
                release_coro = self.governor.release(self.resource, token)
                try:
                    release_task = asyncio.create_task(release_coro)
                except RuntimeError:
                    release_coro.close()
                    raise
                self._release_tasks.add(release_task)
                release_task.add_done_callback(self._release_tasks.discard)
                release_task.add_done_callback(_retrieve_release_error)
                await asyncio.shield(release_task)
            except RuntimeError:
                logger.warning(
                    "release skipped: no running event loop (teardown phase)"
                )

    # ---------- observation half (T9: never blocks the call path) ----------

    async def _finalize(
        self,
        call_id: str,
        *,
        ok: bool,
        result: Any,
        error: BaseException | None,
        cancelled: bool,
        elapsed: float,
        payload_fn: PayloadFn | None = None,
        content_fn: ContentFn | None = None,
        pointer_data: bytes | None = None,
        cost_units: int | None = None,
    ) -> None:
        """Pointer-ize content, then write one audit event."""
        payload: dict[str, Any] = {}
        if ok and payload_fn is not None:
            try:
                payload.update(payload_fn(result) or {})
            except Exception as e:
                logger.warning(f"payload_fn failed (audit continues): {e}")
        if error is not None:
            payload["error"] = str(error)
        if cancelled:
            payload["cancelled"] = True
        payload["elapsed_ms"] = int(elapsed * 1000)
        if cost_units is not None:
            # C2 (v0.1.5): cost_fn output is recorded whenever evaluated,
            # making the documented "cost feeds observation" semantics real.
            payload["cost_units"] = cost_units
        if self._parent_task_id is not None:
            payload["parent_task_id"] = self._parent_task_id
        if self._execution_id is not None:
            payload["execution_id"] = self._execution_id

        if pointer_data is None and ok and content_fn is not None:
            try:
                pointer_data = content_fn(result)
            except Exception as e:
                logger.warning(f"content_fn failed (audit continues): {e}")
                pointer_data = None
        pointer_payload = await self._maybe_pointerize(pointer_data)
        if pointer_payload is not None:
            payload["pointer"] = pointer_payload

        await self._write_audit(call_id, payload)

    async def _maybe_pointerize(self, data: bytes | None) -> dict | None:
        if data is None or self._content_writer is None:
            return None
        try:
            pointer = await self._content_writer.put(
                data, content_type=self._content_type
            )
            return pointer.to_payload()
        except Exception as e:
            logger.warning(f"content pointer-ize failed (audit continues): {e}")
            return None

    async def _write_audit(self, call_id: str, payload: dict) -> None:
        if self._audit_writer is None:
            return
        try:
            await self._audit_writer.append(
                AuditEvent(
                    event_id=call_id,
                    task_id=self._task_id or "",
                    scope=self._scope,
                    event_type=self._event_type,
                    payload=payload,
                )
            )
        except Exception as e:
            logger.warning(f"audit append failed (call unaffected): {e}")