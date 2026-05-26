"""Idempotencia distribuida con Redis SET NX.

Primitiva del core, NO responsabilidad de cada handler. El BaseConsumer
chequea idempotencia antes de invocar `process_message` — si el evento ya
fue procesado, hace commit del offset y sigue, sin invocar el handler.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from redis.asyncio import Redis


class IdempotencyStore:
    """Marca eventos como procesados usando Redis SET NX con TTL.

    Args:
        redis: cliente Redis async.
        namespace: prefijo de las keys (típicamente el nombre del consumer).
        ttl_seconds: cuánto retener la marca. Default 7 días — suficiente para
            que un reprocesamiento masivo (rebalance + reset offset) detecte
            duplicados, pero no acumula keys eternamente.
    """

    def __init__(
        self,
        redis: Redis,
        *,
        namespace: str,
        ttl_seconds: int = 7 * 24 * 3600,
    ) -> None:
        self._redis = redis
        self._namespace = namespace
        self._ttl = ttl_seconds

    def _key(self, event_id: str) -> str:
        return f"idempotency:{self._namespace}:{event_id}"

    async def claim(self, event_id: str) -> bool:
        """Intenta marcar `event_id` como procesado.

        Returns:
            True si la marca se hizo (evento nuevo, procesar).
            False si ya existía (evento duplicado, saltar).
        """
        result = await self._redis.set(self._key(event_id), "1", nx=True, ex=self._ttl)
        return bool(result)

    async def has_been_processed(self, event_id: str) -> bool:
        """Chequea sin marcar — para tests / debugging."""
        return bool(await self._redis.exists(self._key(event_id)))

    async def release(self, event_id: str) -> None:
        """Borra la marca. Usar SOLO en tests o si el handler falla y querés
        permitir reintento sin tener que esperar el TTL."""
        await self._redis.delete(self._key(event_id))
