"""EnrichManager: placeholder dispatch and settle window (cancel_token bound per stream).

Hooks into StreamExecutor's on_hit:
1. Allocate placeholder_id and backfill hit
2. Emit enrich.marker / enrich.placeholder events (via mux, with char_offset)
3. Register record (with char_offset), dispatch tasks according to enrich_mode
   - local:    asyncio.create_task calls enricher, writes result back to registry
   - taskflow: B6 adapter (task_ref prefix tf:)
4. settle(timeout): wait for completed enrichments within window and emit enrich.resolved events
5. cancel_all(): cancel all enrich tasks
6. cancel_placeholder(pid): cancel a single enrich task by placeholder_id

Cancel_token changed from "global single token" to
stream_id → token mapping; when dispatching, take the token of the stream to which the placeholder belongs —
fixes the misalignment where canceling the second stream would not signal its enrich tasks in multi-substream scenarios.
"""
from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable

from orditect.stream.config import EnrichMode, StreamConfig
from orditect.stream.core import CancellationToken
from orditect.stream.enrich.placeholder import (
    PlaceholderRecord,
    PlaceholderRegistry,
)
from orditect.stream.events import (
    EventType,
    PlaceholderState,
    make_enrich_marker,
    make_enrich_placeholder,
    make_enrich_resolved,
)
from orditect.stream.mux import StreamMux
from orditect.stream.pipeline import MarkerHit
from orditect.stream.protocols import (
    EnricherProtocol,
    EnrichRequest,
    StreamHooks,
)
from orditect.stream.utils import new_local_job_id, new_placeholder_id

logger = logging.getLogger(__name__)


class EnrichManager:
    """Enrich dispatcher."""

    def __init__(
        self,
        enricher: EnricherProtocol,
        mux: StreamMux,
        config: StreamConfig,
        registry: PlaceholderRegistry | None = None,
        loading_url: str = "",
        hooks: StreamHooks | None = None,
        cancel_tokens: dict[str, CancellationToken] | None = None,
    ):
        """
        Args:
            cancel_tokens: stream_id → CancellationToken mapping
                (filled by runner in run(), this manager holds the same reference).
                When dispatching, take the token of the stream to which the placeholder belongs;
                if no mapping, it is None (enricher receives cancel_token=None, does not perceive cancellation).
        """
        self._enricher = enricher
        self._mux = mux
        self._cfg = config
        self._registry = registry or PlaceholderRegistry()
        self._loading_url = loading_url
        self._hooks = hooks
        self._cancel_tokens = cancel_tokens if cancel_tokens is not None else {}
        self._enrich_tasks: dict[str, asyncio.Task] = {}

    @property
    def registry(self) -> PlaceholderRegistry:
        return self._registry

    # ---- on_hit hook (injected by StreamExecutor) ----
    async def on_hit(self, stream_id: str, stage: str, hit: MarkerHit) -> None:
        """Marker hit: allocate id, emit events, register, dispatch."""
        placeholder_id = new_placeholder_id()
        hit.placeholder_id = placeholder_id

        # enrich.marker event
        await self._mux.emit(
            stream_id, EventType.ENRICH_MARKER,
            make_enrich_marker(placeholder_id=placeholder_id, context_text=hit.context_text),
            stage=stage,
        )
        # enrich.placeholder event (with char_offset, P0)
        await self._mux.emit(
            stream_id, EventType.ENRICH_PLACEHOLDER,
            make_enrich_placeholder(
                placeholder_id=placeholder_id,
                loading_url=self._loading_url,
                char_offset=hit.char_offset,
            ),
            stage=stage,
        )
        await self._call_hook("on_marker", stream_id, placeholder_id)

        # register (record with char_offset and stage, P0; task_ref follows deterministic convention)
        task_ref = self._make_task_ref(placeholder_id)
        record = PlaceholderRecord(
            placeholder_id=placeholder_id,
            stream_id=stream_id,
            stage=stage,
            context_text=hit.context_text,
            loading_url=self._loading_url,
            task_ref=task_ref,
            char_offset=hit.char_offset,
        )
        await self._registry.register(record)

        # dispatch (v0.3.0: bind cancel_token of the owning stream)
        stream_token = self._cancel_tokens.get(stream_id)
        task = asyncio.create_task(self._dispatch_local(record, stream_token))
        self._enrich_tasks[placeholder_id] = task

    def _make_task_ref(self, placeholder_id: str) -> str:
        """Generate task_ref (deterministic ID convention, shared with TaskflowEnricher).

        Framework specification: taskflow mode task_id = f"enrich-{placeholder_id}" —
        aligns dispatcher and reference side (ManifestResolver) zero-channel; retries/replays
        for the same placeholder converge to the same task_id (idempotent, leveraging taskstore idempotent primitives).
        Local mode retains local: prefix as provenance identifier (no delegation semantics).
        """
        if self._cfg.enrich_mode is EnrichMode.TASKFLOW:
            return f"tf:enrich-{placeholder_id}"
        return f"local:{placeholder_id}"

    async def _dispatch_local(
        self,
        record: PlaceholderRecord,
        cancel_token: CancellationToken | None = None,
    ) -> None:
        """Local mode: call enricher in local coroutine, write result back to registry."""
        try:
            req = EnrichRequest(
                placeholder_id=record.placeholder_id,
                context_text=record.context_text,
                stream_id=record.stream_id,
                stage=record.stage,
            )
            result = await self._enricher.resolve(req, cancel_token=cancel_token)
            if result.state is PlaceholderState.RESOLVED:
                await self._registry.mark_resolved(record.placeholder_id, result.url, result.meta)
            else:
                await self._registry.mark_failed(
                    record.placeholder_id, "enricher returned failed", self._loading_url
                )
        except asyncio.CancelledError:
            # enrich task cancelled: mark failed so manifest truthfully reflects,
            # and immediately release settle's wait_one
            await self._registry.mark_failed(
                record.placeholder_id, "cancelled by user", self._loading_url
            )
            raise
        except Exception as e:
            await self._registry.mark_failed(record.placeholder_id, str(e), self._loading_url)
        finally:
            self._enrich_tasks.pop(record.placeholder_id, None)

    # ---- settle window ----
    async def settle(self, timeout: float) -> None:
        """Wait for settle window: when resolved within window, emit enrich.resolved events.

        Timeout handling branches by mode:
        - local mode: no delegation channel; upon timeout, mark failed + fallback_url (loading image),
          truthfully reflected in manifest. Previously kept pending with the illusion that "client can poll
          using local: reference", but that namespace always fails resolution on resolver side (job_id used
          to query stream_id's manifest) — delegation channel does not exist, so stop misleading.
        - taskflow mode: keep pending (manifest annotates tf: reference, client ManifestResolver polls
          by deterministic task_id).
        """
        if timeout <= 0:
            return
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        for rec in list(self._registry.pending()):
            remaining = deadline - loop.time()
            if remaining <= 0:
                break
            final = await self._registry.wait_one(rec.placeholder_id, remaining)
            if final and final.state is PlaceholderState.RESOLVED and final.url:
                await self._mux.emit(
                    final.stream_id, EventType.ENRICH_RESOLVED,
                    make_enrich_resolved(
                        placeholder_id=final.placeholder_id,
                        url=final.url,
                        state=PlaceholderState.RESOLVED,
                    ),
                    stage=final.stage,
                )
                await self._call_hook(
                    "on_resolved", final.stream_id, final.placeholder_id,
                    final.elapsed() or 0.0,
                )

        # v0.3.2: local mode timeout marks failed (no delegation channel, reflect truthfully)
        if self._cfg.enrich_mode is not EnrichMode.TASKFLOW:
            for rec in self._registry.pending():
                await self._registry.mark_failed(
                    rec.placeholder_id,
                    "settle timeout (no delegation channel in local mode)",
                    self._loading_url,
                )

    # ---- cancel interface ----
    async def cancel_all(self) -> None:
        """Cancel all enrich tasks (CancelledError branch marks failed, settle proceeds immediately)."""
        tasks = [t for t in self._enrich_tasks.values() if not t.done()]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._enrich_tasks.clear()

    async def cancel_placeholder(self, placeholder_id: str) -> bool:
        """Cancel a single enrich task by placeholder_id (fine-grained control)."""
        task = self._enrich_tasks.get(placeholder_id)
        if task is None or task.done():
            return False
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        return True

    async def _call_hook(self, method: str, *args) -> None:
        if not self._hooks:
            return
        try:
            fn = getattr(self._hooks, method, None)
            if fn:
                await fn(*args)
        except Exception:
            pass