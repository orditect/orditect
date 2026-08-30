"""Behavior-pinning tests for the v0.1.1 dependency-governance primitives.

Requires a real Redis (default db15, per the pinning-suite convention).
Pins: counter semantics (incl. negative fault tolerance), attached-key TTL
discipline, MULTI/EXEC vote atomicity, and ready-scan filtering.
"""

from __future__ import annotations

import asyncio

import pytest
import pytest_asyncio

from orditect.core import TaskRedisDB

REDIS_URL = "redis://localhost:6379/15"

pytestmark = pytest.mark.pinning


@pytest_asyncio.fixture
async def db():
    instance = TaskRedisDB(redis_url=REDIS_URL)
    await instance.connect()
    await instance.client.flushdb()
    yield instance
    await instance.client.flushdb()
    await instance.close()


# ----- counter semantics -----


async def test_remaining_deps_set_get_decr(db):
    await db.initialize_task("t1", expiry=100)
    await db.set_remaining_deps("t1", 3)
    assert await db.get_remaining_deps("t1") == 3
    assert await db.decr_remaining_deps("t1") == 2
    assert await db.decr_remaining_deps("t1") == 1
    assert await db.get_remaining_deps("t1") == 1


async def test_decr_missing_key_returns_negative(db):
    # fault-tolerance contract: DECR on a missing key yields -1, no exception
    assert await db.decr_remaining_deps("ghost") == -1

# FLIP(v0.1.5): a ghost counter materialized by DECR on a missing key gets
# a fallback TTL when it exists (owner gone) — never eternal; a key that
# does not exist is never materialized by the sync itself.
async def test_decr_ghost_counter_gets_fallback_ttl(db, redis_client):
    """DECR on a missing key materializes a counter; with the owner gone it
    must carry a TTL (never eternal), because the DECR itself made it real
    state."""
    await db.client.delete("task:ghost2")
    assert await db.decr_remaining_deps("ghost2") == -1
    ttl = await db.client.ttl("task:ghost2:remaining_deps")
    assert ttl > 0, "materialized counter must not be eternal"
# ----- TTL discipline -----


async def test_attached_keys_share_hot_record_ttl(db):
    await db.initialize_task("parent", expiry=100)
    await db.initialize_task("child", expiry=100)
    await db.sadd_active_child("parent", "child")
    await db.set_remaining_deps("child", 1)
    await db.vote_and_check_threshold("child", "parent", 2)
    await db.sadd_result_consumer("child", "c1")

    for key in (
        "task:parent:active_children",
        "task:child:remaining_deps",
        "task:child:cancel_votes",
        "task:child:result_consumers",
    ):
        ttl = await db.client.ttl(key)
        assert 0 < ttl <= 100, f"{key} ttl={ttl}"


async def test_update_task_advances_attached_ttl(db):
    await db.initialize_task("child", expiry=100)
    await db.set_remaining_deps("child", 1)
    await db.update_task("child", {"status": "in_progress"}, expiry=500)
    ttl = await db.client.ttl("task:child:remaining_deps")
    assert 100 < ttl <= 500


async def test_update_task_preserve_mode_keeps_attached_ttl(db):
    await db.initialize_task("child", expiry=300)
    await db.set_remaining_deps("child", 1)
    # preserve mode (expiry=None): attached TTL must NOT jump to the 7-day default
    await db.update_task("child", {"status": "in_progress"})
    ttl = await db.client.ttl("task:child:remaining_deps")
    assert 0 < ttl <= 300


# ----- vote atomicity -----


async def test_vote_repeat_same_parent_not_double_counted(db):
    await db.initialize_task("child", expiry=100)
    assert await db.vote_and_check_threshold("child", "p1", 2) is False
    assert await db.vote_and_check_threshold("child", "p1", 2) is False  # idempotent
    assert await db.get_cancel_votes("child") == ["p1"]
    assert await db.vote_and_check_threshold("child", "p2", 2) is True


async def test_concurrent_votes_exactly_one_winner(db):
    await db.initialize_task("child", expiry=100)
    parents = [f"p{i}" for i in range(3)]
    results = await asyncio.gather(
        *(db.vote_and_check_threshold("child", p, 3) for p in parents)
    )
    assert results.count(True) == 1
    assert sorted(await db.get_cancel_votes("child")) == parents


async def test_clear_cancel_votes(db):
    await db.initialize_task("child", expiry=100)
    await db.vote_and_check_threshold("child", "p1", 5)
    assert await db.get_cancel_votes("child") == ["p1"]
    await db.clear_cancel_votes("child")
    assert await db.get_cancel_votes("child") == []


# ----- active-children set -----


async def test_active_children_add_list_remove(db):
    await db.initialize_task("parent", expiry=100)
    await db.sadd_active_child("parent", "c1")
    await db.sadd_active_child("parent", "c2")
    assert await db.get_active_children("parent") == ["c1", "c2"]
    await db.srem_active_child("parent", "c1")
    assert await db.get_active_children("parent") == ["c2"]


# ----- result-consumer dedup -----


async def test_result_consumer_dedup(db):
    await db.initialize_task("t1", expiry=100)
    assert await db.sadd_result_consumer("t1", "consumer-1") is True
    assert await db.sadd_result_consumer("t1", "consumer-1") is False
    assert await db.sadd_result_consumer("t1", "consumer-2") is True


# ----- ready scan -----


async def test_list_ready_dep_tasks_counter_only(db):
    await db.initialize_task("a", expiry=100, initial_status="pending")
    await db.initialize_task("b", expiry=100, initial_status="pending")
    await db.set_remaining_deps("a", 0)
    await db.set_remaining_deps("b", 1)
    assert await db.list_ready_dep_tasks() == ["a"]
    await db.decr_remaining_deps("b")
    assert await db.list_ready_dep_tasks() == ["a", "b"]


async def test_list_ready_dep_tasks_with_status_filter(db):
    await db.initialize_task("a", expiry=100, initial_status="pending")
    await db.set_remaining_deps("a", 0)
    assert await db.list_ready_dep_tasks(status="pending") == ["a"]
    await db.update_task("a", {"status": "in_progress"})
    assert await db.list_ready_dep_tasks(status="pending") == []
    # ghost candidate: set_remaining_deps materializes a counter without a
    # hot record; it is kept (with a fallback TTL) and shows up unfiltered,
    # but is excluded by the status filter (no hot record to match).
    await db.set_remaining_deps("ghost", 0)
    assert "ghost" in await db.list_ready_dep_tasks()
    assert "ghost" not in await db.list_ready_dep_tasks(status="pending")