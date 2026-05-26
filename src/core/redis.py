"""Factory para clientes Redis async."""

from __future__ import annotations

from redis.asyncio import Redis, from_url


class RedisClientFactory:
    """Crea clientes Redis async configurados consistentemente.

    Mantiene un pool por URL para que múltiples llamadas a `client()`
    reusen conexiones.
    """

    def __init__(self) -> None:
        self._clients: dict[str, Redis] = {}

    def client(self, url: str, *, decode_responses: bool = True) -> Redis:
        """Devuelve un cliente Redis async, reusando pool por URL."""
        key = f"{url}|{decode_responses}"
        if key not in self._clients:
            self._clients[key] = from_url(
                url,
                decode_responses=decode_responses,
                health_check_interval=30,
                socket_keepalive=True,
            )
        return self._clients[key]

    async def close_all(self) -> None:
        """Cierra todos los clientes — llamar en `on_stop()` del consumer."""
        for client in self._clients.values():
            await client.aclose()
        self._clients.clear()
