"""ManifestResolver: polls placeholder results by task_ref from manifest.

Local namespace deprecated (local mode has no delegation channel, timeout means failed — pending local references no longer appear in manifest). If a local: reference is encountered (from old manifest leftovers), terminate immediately and return None, no longer poll for 300s.
For tf: namespace, fetch task by deterministic ID: task_id = task_ref[3:], extract from task record result.url (consistent with TaskflowEnricher contract).

Query function is injected by caller (framework does not directly depend on taskflow / HTTP client).
"""
from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

# taskflow query function: task_id → {"status":..., "result": {...}} | None
TaskflowQueryFn = Callable[[str], Awaitable[dict[str, Any] | None]]
# result callback: placeholder_id, url (or None=failure)
ResolveCallback = Callable[[str, str | None], Awaitable[None]]

# polling result sentinel
_CONTINUE = object()   # 未完成，继续等
_FAILED = object()     # 失败/终止，立即停止


class ManifestResolver:
    """Manifest resolution delegate."""

    def __init__(
        self,
        taskflow_query: TaskflowQueryFn | None = None,
        poll_interval: float = 1.0,
        max_wait: float = 300.0,
    ):
        """
        Args:
            taskflow_query: task query function for tf: namespace
                (task_id -> task record dict or None)
            poll_interval: polling interval (seconds)
            max_wait: maximum wait per placeholder (seconds)
        """
        self._tf_query = taskflow_query
        self._poll = poll_interval
        self._max_wait = max_wait

    async def resolve_all(
        self,
        manifest: dict[str, Any],
        on_resolved: ResolveCallback,
    ) -> None:
        """Resolve all pending placeholders in manifest, poll concurrently."""
        pendings = [
            ph for ph in manifest.get("placeholders", [])
            if ph.get("state") == "pending"
        ]
        if not pendings:
            return
        await asyncio.gather(*(
            self._resolve_one(ph, on_resolved) for ph in pendings
        ))

    async def _resolve_one(self, ph: dict[str, Any], on_resolved: ResolveCallback) -> None:
        task_ref = ph.get("task_ref", "")
        placeholder_id = ph.get("placeholder_id", "")
        url = await self._poll_task(task_ref)
        await on_resolved(placeholder_id, url)

    async def _poll_task(self, task_ref: str) -> str | None:
        """Poll by namespace, return actual url or None (failure/timeout)."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._max_wait

        while loop.time() < deadline:
            result = await self._query_once(task_ref)
            if result is _CONTINUE:
                await asyncio.sleep(self._poll)
                continue
            if result is _FAILED:
                return None
            return result  # 成功拿到 url
        return None  # 超时

    async def _query_once(self, task_ref: str) -> str | object:
        """Single query.

        Returns:
            url string:  success
            _CONTINUE:   not complete, continue polling
            _FAILED:     failed/terminated, stop immediately
        """
        if task_ref.startswith("tf:") and self._tf_query:
            # deterministic ID convention: task_id is task_ref without prefix
            task_id = task_ref[3:]
            data = await self._tf_query(task_id)
            if data is None:
                return _CONTINUE
            status = data.get("status")
            if status == "succeeded":
                result = data.get("result") or {}
                return result.get("url") or result.get("image_oss") or _FAILED
            if status in ("failed", "cancelled"):
                return _FAILED
            return _CONTINUE  # pending/queued/running 等，继续等

        return _FAILED