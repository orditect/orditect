"""Storage factory (taskbase promoted to hard dependency, directly returns TaskRedisDB)."""
import logging

import redis.asyncio as aioredis

from orditect.core import TaskRedisDB
from orditect.flow.protocols.storage import TaskStorageProtocol

logger = logging.getLogger(__name__)

# : taskflow state machine transition table (R10 minesweeper: passed to taskbase, replacing its default table)
TASKFLOW_TRANSITIONS: dict[str, set[str]] = {
    "": {"pending", "queued"},
    "pending": {"queued", "cancelled"},
    "queued": {"running", "cancelled"},
    "running": {"succeeded", "failed", "cancelled"},
    "succeeded": set(),
    "failed": set(),
    "cancelled": set(),
}

# : taskflow terminal state set (passed to taskbase task_update.lua ARGV[6])
TASKFLOW_TERMINAL_STATUSES: tuple[str, ...] = ("succeeded", "failed", "cancelled")


def get_default_storage(redis_client: aioredis.Redis) -> TaskStorageProtocol:
    """Return taskbase TaskRedisDB (taskflow vocabulary wired).

        taskbase is now a hard dependency; local storage implementations have been removed,
        and the factory is simplified to directly return TaskRedisDB.

        Note: The returned TaskRedisDB requires the caller to call connect() (to register Lua scripts).
        In DI mode, connect() only registers scripts and does not rebuild the connection pool.
        """
    logger.info("Using orditect-core TaskRedisDB as storage backend")
    return TaskRedisDB(
        client=redis_client,
        terminal_statuses=TASKFLOW_TERMINAL_STATUSES,
        transitions=TASKFLOW_TRANSITIONS,
    )