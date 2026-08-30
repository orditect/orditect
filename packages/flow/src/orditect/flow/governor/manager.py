"""Business-facing resource status query interface (unified orchestration-layer exit).

Query strategy (consistent with upgrade checklist: call taskbase query interface, fallback to direct governor query):
1. Prefer using orditect-core's LimiterRegistry query interface
   (get_semaphore_status / get_all_semaphore_status),
   hit only when the resource is registered in the registry;
2. If not hit (taskbase not installed / resource not registered), fallback to direct governor query
   (any implementation with get_limit/list_resources duck-typed extensions).
Return format aligned with the three-framework unified standard:
    {
        "resource": "default_stream_llm",
        "limit": 30,
        "usage": 15,           # approximate, for monitoring display only, not for alerting
        "available": 15,
        "utilization": "50.0%",
    }

Usage example:
    manager = GovernorManager(governor)

    # Single resource status
    status = await manager.get_resource_status("task_execution")

    # All resources status (for FastAPI endpoint / monitoring dashboard)
    all_status = await manager.get_all_resources()
"""
from __future__ import annotations

import inspect
import logging
from typing import Dict, Optional

from orditect.flow.protocols.governor import ResourceGovernorProtocol

logger = logging.getLogger(__name__)


def _get_taskbase_registry():
    """Attempt to get taskbase's global LimiterRegistry (returns None if not installed)."""
    try:
        from orditect.core import get_registry
        return get_registry()
    except ImportError:
        return None


class GovernorManager:
    """Business-facing resource status query interface.

    Args:
        governor: Resource governance instance (data source for fallback query path)
    """

    def __init__(self, governor: Optional[ResourceGovernorProtocol] = None):
        self.governor = governor

    @staticmethod
    def _format_status(resource: str, limit: int, usage: int) -> dict:
        """Assemble unified return format (with zero-division and non-negative safeguards)."""
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
            resource: Resource name

        Returns:
            Five-field unified format dict (resource/limit/usage/available/utilization)

        Raises:
            ValueError: Resource not found (not in registry nor governor)
        """
        # 1. priority: taskbase registry (only if resource already registered)
        registry = _get_taskbase_registry()
        if registry is not None and registry.has_semaphore(resource):
            status = await registry.get_semaphore_status(resource)
            # taskbase returns "name" key, externally unified to "resource"
            return {
                "resource": resource,
                "limit": status["limit"],
                "usage": status["usage"],
                "available": status["available"],
                "utilization": status["utilization"],
            }

        # 2. degraded: query governor directly
        if self.governor is not None:
            get_limit = getattr(self.governor, "get_limit", None)
            if callable(get_limit):
                # v0.1.6: get_limit may be sync (e.g. local governors) or
                # async — handle both via isawaitable, mirroring the stream
                # StreamGovernorManager (previously a bare await broke sync
                # implementations with TypeError).
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
        - All semaphores registered in taskbase registry (preferred)
        - Resources enumerated by governor (governor.list_resources() duck-typed detection, optional supplement)

        Returns:
            {resource: status_dict} dict; returns {} if no known resources
        """
        result: Dict[str, dict] = {}
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