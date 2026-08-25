"""Manifest assembly + finalizer hook scheduling + ResultStore persistence.

Responsibilities:
- Aggregate stage results from each substream
- Aggregate final placeholder states (resolved→url / failed→fallback / pending→task_ref)
  P0: include char_offset + stage, manifest can fully reconstruct mixed text-image documents
- Invoke finalizer hooks to collect business ext (category/suggestion etc.)
- Assemble manifest payload
- Write to ResultStore (refetch data source)
"""
from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from orditect.stream.enrich.placeholder import PlaceholderRegistry
from orditect.stream.events import (
    ManifestPlaceholder,
    PlaceholderState,
    StageResultPayload,
    make_manifest,
)
from orditect.stream.protocols import ResultStoreProtocol
from orditect.stream.stream_result import StreamResult  # 修改：从独立模块导入

# finalizer hook: receives all aggregated data, returns business ext dict (merged into manifest.ext)
FinalizerHook = Callable[[dict[str, StreamResult], PlaceholderRegistry], Awaitable[dict[str, Any]]]


class ManifestBuilder:
    """Manifest assembler."""

    def __init__(
        self,
        store: ResultStoreProtocol,
        result_ttl: int = 86400,
        hooks: list[FinalizerHook] | None = None,
    ):
        self._store = store
        self._ttl = result_ttl
        self._hooks = hooks or []

    async def build(
        self,
        stream_results: dict[str, StreamResult],
        registry: PlaceholderRegistry,
        usage: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Assemble manifest payload.

        For multiple substreams, stages are grouped by stream_id: {stream_id:stage_name: result}.
        For a single substream, keep {stage_name: result} flat (compatible with legacy frontend).
        """
        # stages aggregation
        if len(stream_results) == 1:
            only = next(iter(stream_results.values()))
            stages_payload = {
                name: StageResultPayload(content=o.content, thinking=o.thinking or None)
                for name, o in only.stages.items()
            }
        else:
            stages_payload = {
                f"{sid}:{name}": StageResultPayload(content=o.content, thinking=o.thinking or None)
                for sid, sr in stream_results.items()
                for name, o in sr.stages.items()
            }

        # placeholders final state (P0: with char_offset + stage)
        ph_payloads: list[ManifestPlaceholder] = []
        for rec in registry.all():
            ph_payloads.append(
                ManifestPlaceholder(
                    placeholder_id=rec.placeholder_id,
                    task_ref=rec.task_ref,
                    state=rec.state,
                    stage=rec.stage,               # P0
                    char_offset=rec.char_offset,   # P0
                    fallback_url=rec.fallback_url or (rec.loading_url if rec.state is PlaceholderState.FAILED else None),
                    url=rec.url if rec.state is PlaceholderState.RESOLVED else None,
                )
            )

        # errors summary
        errors: list[dict[str, Any]] = []
        for sr in stream_results.values():
            errors.extend(sr.errors)

        # finalizer hooks → ext
        ext: dict[str, Any] = {}
        for hook in self._hooks:
            try:
                part = await hook(stream_results, registry)
                if part:
                    ext.update(part)
            except Exception:
                pass  # 钩子失败不阻塞 manifest

        manifest = make_manifest(
            stages=stages_payload,
            placeholders=ph_payloads,
            usage=usage,
            errors=errors,
            ext=ext,
        )

        # persistence (store one full manifest per stream_id)
        for sid in stream_results:
            try:
                await self._store.save(sid, manifest, self._ttl)
            except Exception:
                pass  # 存储失败不影响主流

        return manifest