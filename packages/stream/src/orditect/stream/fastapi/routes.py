"""Refetch route: endpoint for client to pull manifest in local store mode.
Not needed in taskflow mode (client uses taskflow query interface).
"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException

from orditect.stream.protocols import ResultStoreProtocol


def make_refetch_router(
    store: ResultStoreProtocol,
    prefix: str = "/taskstream",
) -> APIRouter:
    """Create refetch route.
    GET {prefix}/streams/{stream_id} → manifest (404 if not found/expired)
    """
    router = APIRouter(prefix=prefix, tags=["taskstream"])

    @router.get("/streams/{stream_id}")
    async def get_stream_manifest(stream_id: str):
        manifest = await store.get(stream_id)
        if manifest is None:
            raise HTTPException(status_code=404, detail=f"stream not found or expired: {stream_id}")
        return manifest

    return router