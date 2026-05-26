"""Test de integración para IdempotencyStore — Redis real."""

from __future__ import annotations

import pytest
from redis.asyncio import Redis

from src.core.idempotency import IdempotencyStore

pytestmark = pytest.mark.integration


class TestIdempotencyStore:
    async def test_first_claim_succeeds(self, redis_client: Redis) -> None:
        store = IdempotencyStore(redis_client, namespace="test")
        assert await store.claim("event-1") is True

    async def test_second_claim_fails(self, redis_client: Redis) -> None:
        store = IdempotencyStore(redis_client, namespace="test")
        await store.claim("event-1")
        assert await store.claim("event-1") is False

    async def test_namespace_isolation(self, redis_client: Redis) -> None:
        store_a = IdempotencyStore(redis_client, namespace="consumer-a")
        store_b = IdempotencyStore(redis_client, namespace="consumer-b")
        assert await store_a.claim("event-1") is True
        # Mismo event_id, distinto namespace → no colisiona
        assert await store_b.claim("event-1") is True

    async def test_release_allows_reclaim(self, redis_client: Redis) -> None:
        store = IdempotencyStore(redis_client, namespace="test")
        await store.claim("event-1")
        await store.release("event-1")
        assert await store.claim("event-1") is True

    async def test_has_been_processed(self, redis_client: Redis) -> None:
        store = IdempotencyStore(redis_client, namespace="test")
        assert await store.has_been_processed("event-1") is False
        await store.claim("event-1")
        assert await store.has_been_processed("event-1") is True
