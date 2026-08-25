"""StreamRunner: complete stream lifecycle assembly for a single request.

Execution model:
- Producers: N substreams (max_id) run StreamExecutor concurrently → settle → finalizer
- Consumers: mux.events() → produce (envelope, event_type) to upper layer (fastapi layer encodes SSE)
- Disconnect: DisconnectMonitor policy execution (cancel/grace/continue)
- Cancel: cancel() active interruption (stop output, semaphore delayed release)
- Force cancel: cancel(force=True) cancels executor coroutine (immediately release semaphore)

External API:
  runner.run() -> AsyncIterator[(EventEnvelope, EventType)]
  runner.notify_disconnect() / notify_reconnect()
  runner.cancel() / get_partial_content()

SSE encoding is not here (fastapi layer), runner produces standard event stream.
    Contract declaration:
    - Single-use object: run() is not re-entrant (mux/executors/cancel_tokens are initialized inside run()),
      create a new instance for repeated execution.
    - cancel() must be called after run() starts (_cancel_tokens are created inside run()),
      calling before run() is silently ignored (considered out-of-contract usage).

    """

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator

from orditect.stream.config import DisconnectPolicy, StreamConfig
from orditect.stream.core import CancellationToken
from orditect.stream.disconnect import DisconnectMonitor, GraceBuffer
from orditect.stream.enrich import EnrichManager, PlaceholderRegistry
from orditect.stream.events import (
    EventEnvelope,
    EventType,
    make_stream_cancelled,
    make_stream_end,
    make_stream_start,
)
from orditect.stream.finalizer import ManifestBuilder
from orditect.stream.mux import StreamMux
from orditect.stream.protocols import (
    EnricherProtocol,
    ResultStoreProtocol,
    StreamHooks,
)
from orditect.stream.protocols.governor import ResourceGovernorProtocol
from orditect.stream.runner.stream import StreamExecutor
from orditect.stream.stages import StageConfig
from orditect.stream.stream_result import StreamResult
from orditect.stream.utils import new_resume_token, new_stream_id
import logging
logger = logging.getLogger(__name__)

class StreamRunner:
    """Stream runner for a single request."""

    def __init__(
        self,
        stages: list[StageConfig],
        enricher: EnricherProtocol,
        store: ResultStoreProtocol,
        config: StreamConfig,
        *,
        manifest_builder: ManifestBuilder | None = None,
        governor: ResourceGovernorProtocol | None = None,  # 新增
        max_id: int = 1,
        loading_url: str = "",
        finalizer_hooks: list | None = None,
        hooks: StreamHooks | None = None,
    ):
        if max_id < 1:
            raise ValueError("max_id must be >= 1")
        self._stages = stages
        self._cfg = config
        self._max_id = max_id
        self._hooks = hooks
        self._governor = governor  # 新增

        # mux
        self._mux = StreamMux(
            maxsize=config.queue_maxsize,
            backpressure=config.backpressure,
        )
        # enrich
        self._registry = PlaceholderRegistry()
        # v0.3.0 (4): cancel_tokens mapping created in __init__, filled per stream in run(),
        # EnrichManager holds same reference, dispatches using token of placeholder's stream
        self._cancel_tokens: dict[str, CancellationToken] = {}
        self._partial_contents: dict[str, str] = {}
        self._enrich_manager = EnrichManager(
            enricher=enricher,
            mux=self._mux,
            config=config,
            registry=self._registry,
            loading_url=loading_url,
            hooks=hooks,
            cancel_tokens=self._cancel_tokens,  # 共享映射引用
        )
        # finalizer (dependency injection, create default if not provided)
        self._manifest_builder = manifest_builder or ManifestBuilder(
            store=store,
            result_ttl=config.result_ttl,
            hooks=finalizer_hooks,
        )
        # disconnect
        self._monitor = DisconnectMonitor(
            config=config,
            on_cancel=self._on_cancel,
            grace_buffer=GraceBuffer(),
        )

        self._stream_ids: list[str] = []
        self._executors: list[StreamExecutor] = []
        self._executor_tasks: list[asyncio.Task] = []
        self._started = False  # v0.3.2（#24）：一次性对象防护

    # ---- disconnect interface (called by fastapi layer) ----
    async def notify_disconnect(self) -> None:
        await self._monitor.notify_disconnect()

    async def notify_reconnect(self) -> tuple[list, bool]:
        return await self._monitor.notify_reconnect()

    @property
    def should_buffer(self) -> bool:
        return self._monitor.should_buffer()

    @property
    def grace_buffer(self) -> GraceBuffer:
        return self._monitor.buffer

    # ---- cancel interface ----
    async def cancel(
        self,
        stream_id: str | None = None,
        reason: str | None = None,
        force: bool = False,
    ) -> dict[str, str]:
        """Actively cancel stream (returns partial_content for synchronous printing).

        Two modes:
        - force=False (default, graceful mode): mark cancelled, stop output, but continue consuming LLM
          until completion (semaphore held until LLM truly ends).
          Use case: user actively interrupts, LLM connection remains intact.
        - force=True (force mode): on top of graceful mode, cancel executor coroutine,
          semaphore released immediately (StageRunner finally).
          Use case: resource pressure or LLM hangs, need to release semaphore immediately.

        Return value (Ctrl+C emergency channel):
            {stream_id: partial_content} mapping - under Ctrl+C scenario
            event loop is about to close, mux consumption is unreliable, caller can directly print using return value.
            Under normal consumption, mux events are still delivered (dual-channel coexistence).

        Note (force=True semantics):
        - Stream lifecycle continues to manifest/end (keeping the protocol discipline that "stream.end is the only terminal signal"),
          cancelled substreams in manifest are recorded as CANCELLED error.
        - Enrich background tasks are not cancelled here, managed separately by EnrichManager's cancel interface
          (cancel_all / cancel_placeholder).

        Contract: cancel() must be called after run() starts (_cancel_tokens created inside run()),
        calling before run() is silently ignored (returns empty dict).

        Args:
            stream_id: specify which substream to cancel; None cancels all
            reason: cancellation reason (recorded in event)
            force: whether to forcibly cancel executor coroutine (immediately release semaphore)

        Returns:
            {stream_id: partial_content} mapping (returns empty dict if no target stream)
        """
        reason = reason or "cancelled by user"
        partials: dict[str, str] = {}

        # target stream set (single or all)
        target_sids = [stream_id] if stream_id else list(self._cancel_tokens.keys())

        for sid in target_sids:
            # 1. collect partial first (synchronous read, always succeeds, unaffected by subsequent exceptions)
            partials[sid] = self._partial_contents.get(sid, "")

            # 2. cancel token (synchronous operation, always succeeds)
            token = self._cancel_tokens.get(sid)
            if token:
                token.cancel(reason)

            # 3. attempt to deliver event (exception isolated — mux closed/full etc. does not affect partials return)
            try:
                await self._emit_cancelled_event(sid)
            except Exception as e:
                logger.warning(
                    f"emit cancelled event failed (partial_content still returned): "
                    f"stream={sid}, error: {e}"
                )

            # 4. force mode cancels execution coroutine (semaphore released immediately)
            if force:
                self._force_cancel_executor(sid)

        return partials

    def _force_cancel_executor(self, stream_id: str) -> None:
        """Forcibly cancel the executor coroutine of the specified substream (immediately release semaphore).

                The coroutine enters StageRunner's finally to complete resource release;
                _produce()'s gather catches CancelledError and records CANCELLED in the manifest.
                """
        for sid, task in zip(self._stream_ids, self._executor_tasks):
            if sid == stream_id and not task.done():
                task.cancel()

    def get_partial_content(self, stream_id: str) -> str:
        """Get partial content at interruption (for business layer to save history).

        Args:
            stream_id: substream ID

        Returns:
            Partial content at interruption (aggregated text)
        """
        return self._partial_contents.get(stream_id, "")

    async def _emit_cancelled_event(self, stream_id: str) -> None:
        """Emit cancellation event (always reachable, even if mux is closed).

        cancel is an active user action, business layer needs to be aware, so:
        - mux not closed: normal emit
        - mux closed: ignore (stream has ended, but cancel status is already recorded in token)
        """
        token = self._cancel_tokens.get(stream_id)
        partial = self._partial_contents.get(stream_id, "")

        # mux not closed: deliver normally
        if not self._mux._closed:
            await self._mux.emit(
                stream_id,
                EventType.STREAM_CANCELLED,
                make_stream_cancelled(
                    reason=token.reason or "cancelled by user",
                    cancelled_at=token.cancelled_at or time.time(),
                    partial_content=partial,
                ),
            )
        # mux closed: cancel state already recorded in token, business layer can query via token

    # ---- main stream ----
    async def run(self) -> AsyncIterator[tuple[EventEnvelope, EventType]]:
        """Execute full stream lifecycle, produce event stream.

        Producer coroutine: substream execution → settle → manifest → end → close mux
        Consumption: for await yields mux events

        Contract (explicit):
        - #24: Single-use object, run() is not re-entrant (re-entry raises RuntimeError;
          previously only docstring declaration, no runtime guard).
        - #25: When consumer terminates early (aclose/break out of iteration), cascade cancel producers -
          grace buffer assumes consumer continues consuming events; if consumption stops, buffering is meaningless,
          directly cancel to prevent runner coroutine hanging until substreams naturally end.
        """
        if self._started:
            raise RuntimeError(
                "StreamRunner is a single-use object: run() cannot be re-entered. "
                "Create a new instance for a new stream."
            )
        self._started = True

        # register substreams
        for _ in range(self._max_id):
            sid = new_stream_id()
            self._stream_ids.append(sid)
            self._cancel_tokens[sid] = CancellationToken()
            self._mux.register(sid)
            await self._call_hook("on_stream_start", sid)
        # stream.start event (one per substream)
        resume_token = new_resume_token()
        for sid in self._stream_ids:
            await self._mux.emit(
                sid, EventType.STREAM_START,
                make_stream_start(
                    stages=[s.name for s in self._stages],
                    resume_token=resume_token,
                    config_echo={
                        "thinking_mode": self._cfg.thinking_mode.value,
                        "enrich_settle_timeout": self._cfg.enrich_settle_timeout,
                    },
                ),
            )

        # start producer
        producer = asyncio.create_task(self._produce())

        # consume mux events
        try:
            async for envelope, event_type in self._mux.events():
                # collect partial content (T6: only aggregate content — thinking not mixed into partial_content)
                if event_type == EventType.STREAM_DELTA:
                    if envelope.data.get("kind") == "content":
                        sid = envelope.stream_id
                        text = envelope.data.get("text", "")
                        self._partial_contents[sid] = self._partial_contents.get(sid, "") + text
                # check cancel (T3: stop business content output after cancel, but terminal signals must be delivered —
                # protocol discipline: "stream.end is the only terminal signal")
                if self._cancel_tokens.get(envelope.stream_id, CancellationToken()).is_cancelled():
                    if event_type not in (
                        EventType.STREAM_CANCELLED,
                        EventType.STREAM_MANIFEST,
                        EventType.STREAM_END,
                    ):
                        continue
                # grace disconnecting: events buffered, not delivered
                if self._monitor.should_buffer:
                    await self._monitor.buffer.put(envelope, event_type)
                    continue
                yield envelope, event_type
        finally:
            # v0.3.2 (#25): consumer terminates early (producer not finished) → cascading cancel.
            # In normal completion path producer already done, fast path.
            if not producer.done():
                await self._on_cancel()  # cancel executors + force_close mux
                try:
                    # hard limit: after cascading, if cleanup still hangs then abandon wait (prevent infinite hang)
                    await asyncio.wait_for(
                        producer, timeout=self._cfg.grace_period + 5.0
                    )
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    logger.error("producer did not finish after cascade cancel")
            else:
                await producer  # 传播生产者异常（正常完成路径）
            await self._monitor.close()

    async def _produce(self) -> None:
        """Producer: concurrently execute substreams → settle → manifest → end → close mux."""
        stream_results: dict[str, StreamResult] = {}
        started_at = time.monotonic()  # v0.3.1：记录起点（on_stream_end 真实时长）
        try:
            # execute substreams concurrently
            self._executors = [
                StreamExecutor(
                    stream_id=sid,
                    stages=self._stages,
                    config=self._cfg,
                    mux=self._mux,
                    on_hit=self._enrich_manager.on_hit,
                    governor=self._governor,  # 新增
                    cancel_token=self._cancel_tokens[sid],  # 新增
                )
                for sid in self._stream_ids
            ]
            self._executor_tasks = [
                asyncio.create_task(ex.run()) for ex in self._executors
            ]
            results = await asyncio.gather(*self._executor_tasks, return_exceptions=True)
            for sid, res in zip(self._stream_ids, results):
                if isinstance(res, asyncio.CancelledError):
                    # substream force-cancelled (cancel(force=True) / disconnect cancel strategy):
                    # CancelledError inherits BaseException, not Exception,
                    # must catch separately, otherwise treated as StreamResult passed to manifest builder
                    sr = StreamResult(stream_id=sid)
                    sr.errors.append({
                        "code": "CANCELLED",
                        "message": "stream cancelled",
                    })
                    stream_results[sid] = sr
                elif isinstance(res, Exception):
                    # substream failed: record error (manifest summary)
                    sr = StreamResult(stream_id=sid)
                    sr.errors.append({"code": "INTERNAL", "message": str(res)})
                    stream_results[sid] = sr
                else:
                    stream_results[sid] = res

            # settle window (enrich fast path backfill in stream)
            await self._enrich_manager.settle(self._cfg.enrich_settle_timeout)

            # finalizer → manifest
            manifest = await self._manifest_builder.build(
                stream_results, self._registry,
            )
            # manifest event (one per substream, content identical)
            for sid in self._stream_ids:
                await self._mux.emit(sid, EventType.STREAM_MANIFEST, manifest)

            # stream.end event (v0.3.1: on_stream_end passes real duration)
            duration = time.monotonic() - started_at
            for sid in self._stream_ids:
                await self._mux.emit(sid, EventType.STREAM_END, make_stream_end())
                await self._call_hook("on_stream_end", sid, duration)

        except Exception as e:
            # producer exception: deliver error event (best effort)
            from orditect.stream.events import ErrorCode, make_stream_error
            for sid in self._stream_ids:
                try:
                    await self._mux.emit(
                        sid, EventType.STREAM_ERROR,
                        make_stream_error(ErrorCode.INTERNAL, str(e), retryable=False),
                    )
                except Exception:
                    pass
                await self._call_hook("on_error", sid, "INTERNAL", str(e))
        finally:
            # close all substreams (trigger mux sentinel)
            for sid in self._stream_ids:
                try:
                    await self._mux.close_stream(sid)
                except Exception:
                    pass

    async def _on_cancel(self) -> None:
        """Cancel policy cascade: cancel executor tasks + force close mux."""
        for task in self._executor_tasks:
            task.cancel()
        await asyncio.gather(*self._executor_tasks, return_exceptions=True)
        await self._mux.force_close()

    async def _call_hook(self, method: str, *args) -> None:
        if not self._hooks:
            return
        try:
            fn = getattr(self._hooks, method, None)
            if fn:
                await fn(*args)
        except Exception:
            pass