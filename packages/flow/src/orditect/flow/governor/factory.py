"""Resource governance factory (taskbase promoted to hard dependency + held distortion fix)."""
import logging
import time
from typing import Optional

import redis.asyncio as aioredis

from orditect.core import get_registry, LeaseToken
from orditect.flow.protocols.governor import ResourceGovernorProtocol

logger = logging.getLogger(__name__)


class TaskbaseGovernorAdapter(ResourceGovernorProtocol):
    """taskbase resource governance adapter (AsyncLeaseSemaphore → ResourceGovernorProtocol).

        The adapter internally maintains a mapping from token.value to the original LeaseToken.
        On release, it retrieves the mapping to restore the real acquired_at — thereby correcting
        the held duration reported by the taskbase on_released hook (Prometheus limiter_hold_duration_seconds).
        Mapping entries are deleted on release; orphan entries that are never released after acquire
        are covered by taskbase's lease expiration mechanism (the mapping itself poses no memory risk:
        the number of semaphore slots in taskbase is finite and bounded by the limit).
        """

    def __init__(self, registry):
        self.registry = registry
        self._tokens: dict[str, LeaseToken] = {}  # token.value → LeaseToken

    async def acquire(self, resource: str, timeout: Optional[float] = None) -> str:
        sem = self.registry.get_semaphore(resource)  # R16：未注册即 KeyError
        token = await sem.acquire(timeout=timeout)
        self._tokens[token.value] = token
        return token.value

    async def try_acquire(self, resource: str) -> Optional[str]:
        sem = self.registry.get_semaphore(resource)
        token = await sem.try_acquire()
        if token is None:
            return None
        self._tokens[token.value] = token
        return token.value

    async def release(self, resource: str, token: str) -> None:
        sem = self.registry.get_semaphore(resource)
        lease_token = self._tokens.pop(token, None)
        if lease_token is None:
            # mapping missing (e.g., adapter releases old token after restart):
            # rebuild LeaseToken, held may be inaccurate but release function correct (Lua ZREM only cares about value)
            lease_token = LeaseToken(
                resource=resource,
                value=token,
                acquired_at=time.monotonic(),
            )
            logger.warning(
                f"LeaseToken mapping missing (held duration will be inaccurate): "
                f"resource={resource}"
            )
        await sem.release(lease_token)

    async def get_usage(self, resource: str) -> int:
        try:
            sem = self.registry.get_semaphore(resource)
            return await sem.in_use()
        except KeyError:
            return 0


def get_default_governor(
        redis_client: Optional[aioredis.Redis] = None,
) -> ResourceGovernorProtocol:
    """Return the taskbase governor adapter.

        taskbase is now a hard dependency; DefaultResourceGovernor has been removed.
        The factory is simplified to directly return the adapter. Resources must be registered
        in LimiterRegistry first (R16: unregistered resources raise KeyError, no implicit creation
        of a pool with limit=10).
        """
    logger.info("Using orditect-core LimiterRegistry as governor backend")
    return TaskbaseGovernorAdapter(get_registry())