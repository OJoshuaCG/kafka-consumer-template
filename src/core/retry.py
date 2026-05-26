"""Retry helpers — backoff exponencial con jitter (anti-thundering-herd)."""

from __future__ import annotations

import asyncio
import random
from collections.abc import Awaitable, Callable

from src.core.exceptions import RetryableError


def backoff_with_jitter(attempt: int, base: float = 1.0, cap: float = 60.0) -> float:
    """Exponential backoff con jitter aleatorio.

    Sin jitter, todos los pods reintentan al mismo tiempo cuando la DB tose,
    y la matan. Con jitter (factor 0.5..1.5), los reintentos se desparraman.

    Args:
        attempt: número de intento, empezando en 0.
        base: delay base en segundos.
        cap: tope máximo del delay (evita esperas absurdas).

    Returns:
        Segundos a esperar antes del próximo intento.
    """
    delay: float = base * (2**attempt) * (0.5 + random.random())
    return min(delay, cap)


async def retry_async[T](
    fn: Callable[[], Awaitable[T]],
    *,
    max_attempts: int = 3,
    base_delay: float = 1.0,
    cap_delay: float = 60.0,
    retry_on: type[Exception] | tuple[type[Exception], ...] = RetryableError,
) -> T:
    """Reintenta una corutina con backoff+jitter.

    Re-lanza la última excepción si se agotan los intentos.
    """
    last_exc: Exception | None = None
    for attempt in range(max_attempts):
        try:
            return await fn()
        except retry_on as exc:
            last_exc = exc
            if attempt == max_attempts - 1:
                break
            await asyncio.sleep(backoff_with_jitter(attempt, base_delay, cap_delay))
    assert last_exc is not None
    raise last_exc
