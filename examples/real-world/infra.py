"""Real Redis hot path (the production trio).

This is the whole point of the second demo: the SAME business code as the
MVP runs against the production hot path — TaskRedisDB (task store),
AsyncLeaseSemaphore via LimiterRegistry (semaphore), AdmissionQuotaRedisDB
(quota). Nothing else changes.
"""

from __future__ import annotations

import redis.asyncio as aioredis

from orditect.core import AdmissionQuotaRedisDB, get_registry
from orditect.flow.governor.factory import TaskbaseGovernorAdapter
from orditect.flow.storage.factory import get_default_storage

from settings import SETTINGS


async def build_hot_path():
    """Create (storage, governor, quota, redis_client) over real Redis.

    Pings once up front so a bad REDIS_URL fails fast at startup instead
    of surfacing later as an obscure first-command error.
    """
    client = aioredis.from_url(SETTINGS.redis_url, decode_responses=True)
    await client.ping()  # fail fast when Redis is unreachable

    storage = get_default_storage(client)
    await storage.connect()

    registry = get_registry()
    registry.register_semaphore(
        "llm", client, limit=SETTINGS.llm_sem_limit, lease_time=60.0
    )
    registry.register_semaphore("task_execution", client, limit=10, lease_time=60.0)
    governor = TaskbaseGovernorAdapter(registry)

    quota = AdmissionQuotaRedisDB(client=client)
    await quota.connect()

    return storage, governor, quota, client
