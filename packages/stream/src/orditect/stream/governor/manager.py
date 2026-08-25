"""Resource status query interface for streaming scenarios (unified output entry point).

Use cases:
- FastAPI endpoint: GET /governor/resources returns status of all resources
- Monitoring dashboard: View resource watermarks for default_stream_llm / vector_search / enrich_task, etc.
- Business layer queries: Real-time usage of a specific resource

Query strategy (aligned with taskflow GovernorManager):
1. Prefer orditect-core's LimiterRegistry query interface
   (get_semaphore_status / get_all_semaphore_status),
   only hits when the resource is already registered in the registry;
2. On miss (taskbase not installed / resource not registered), fallback to directly querying the injected governor.

Fallback notes (differences from taskflow):
- taskstream's ResourceGovernorProtocol does not define get_limit/list_resources,
  the fallback path relies on duck-typed extensions of the injected implementation (e.g., taskflow DefaultResourceGovernor).
- When the injected governor does not support status query, get_resource_status raises ValueError,
  and get_all_resources skips that source.

Return format aligned with the unified standard across three frameworks:
    {
        "resource": "default_stream_llm",
        "limit": 30,
        "usage": 15,           # Approximate, for monitoring display only, not for alerting
        "available": 15,
        "utilization": "50.0%",
    }

Usage examples:
    manager = StreamGovernorManager(governor)

    # Single resource status
    status = await manager.get_resource_status("default_stream_llm")

    # All resources status
    all_status = await manager.get_all_resources()
"""
from __future__ import annotations

import inspect
import logging
from typing import Dict, Optional

from orditect.stream.protocols.governor import ResourceGovernorProtocol

logger = logging.getLogger(__name__)


def _get_taskbase_registry():
    """Attempt to get taskbase's global LimiterRegistry (returns None if not installed)."""
    try:
        from orditect.core import get_registry
        return get_registry()
    except ImportError:
        return None


class StreamGovernorManager:
    """Resource status query interface for streaming scenarios.

    Args:
        governor: Resource governance instance (data source for fallback query path, optional)
    """

    def __init__(self, governor: Optional[ResourceGovernorProtocol] = None):
        self.governor = governor

    @staticmethod
    def _format_status(resource: str, limit: int, usage: int) -> dict:
        """Assemble unified return format (with division-by-zero defense and non-negative defense)."""
        utilization = f"{usage / limit * 100:.1f}%" if limit > 0 else "0.0%"
        return {
            "resource": resource,
            "limit": limit,
            "usage": usage,
            "available": max(0, limit - usage),
            "utilization": utilization,
        }

    async def get_resource_status(self, resource: str) -> dict:
        """Get status of a single resource.

        Args:
            resource: Resource name (e.g., "default_stream_llm")

        Returns:
            Five-field unified format dictionary (resource/limit/usage/available/utilization)

        Raises:
            ValueError: Resource not found (not in registry nor governor)
        """
        # 1. Prefer: taskbase registry (only when resource is registered)
        registry = _get_taskbase_registry()
        if registry is not None and registry.has_semaphore(resource):
            status = await registry.get_semaphore_status(resource)
            # taskbase returns "name" key, convert to "resource" for external interface
            return {
                "resource": resource,
                "limit": status["limit"],
                "usage": status["usage"],
                "available": status["available"],
                "utilization": status["utilization"],
            }

        # 2. Fallback: directly query the injected governor (duck-type probe get_limit)
        if self.governor is not None:
            get_limit = getattr(self.governor, "get_limit", None)
            if callable(get_limit):
                limit = get_limit(resource)
                if inspect.isawaitable(limit):
                    limit = await limit
                usage = await self.governor.get_usage(resource)
                return self._format_status(resource, limit, usage)

            logger.warning(
                f"Governor {type(self.governor).__name__} does not implement "
                f"get_limit(), cannot query resource status: {resource}"
            )

        raise ValueError(
            f"Resource not found: '{resource}' "
            f"(not registered in taskbase registry, "
            f"and governor does not support status query)"
        )

    async def get_all_resources(self) -> Dict[str, dict]:
        """Get status of all known resources.

        Data sources:

            All semaphores registered in taskbase registry (primary)

            Resources enumerated by governor (list_resources() duck-type probe, optional supplement)

        Returns:
        {resource: status_dict} dictionary; returns {} when no known resources exist
        """
        result: Dict[str, dict] = {}

        # 1. taskbase registry (primary data source)
        registry = _get_taskbase_registry()
        if registry is not None:
            all_status = await registry.get_all_semaphore_status()
            for name, status in all_status.items():
                result[name] = {
                    "resource": name,
                    "limit": status["limit"],
                    "usage": status["usage"],
                    "available": status["available"],
                    "utilization": status["utilization"],
                }

        # 2. governor enumeration (degraded supplement; does not override existing registry entries)
        # list_resources may be synchronous (taskflow DefaultResourceGovernor),
        # or asynchronous (future custom implementation), use isawaitable to handle both
        if self.governor is not None:
            list_resources = getattr(self.governor, "list_resources", None)
            if callable(list_resources):
                names = list_resources()
                if inspect.isawaitable(names):
                    names = await names
                for name in names:
                    if name not in result:
                        result[name] = await self.get_resource_status(name)

        return result