"""Handlers del example consumer — UNA función async por tipo de evento.

REGLA: cada handler es una función async PURA. Recibe el evento ya
validado + dependencias (db, redis) inyectadas. NO toca Kafka, NO commitea,
NO decide retry. Eso lo hace el BaseConsumer.

Esto hace que los handlers se testeen sin infra (`tests/unit/consumers/example/`).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from src.consumers.example.schemas import FarewellEvent, GreetingEvent
from src.core.exceptions import NonRetryableError, RetryableError
from src.core.logging import get_logger

if TYPE_CHECKING:
    from src.db.database import Database

logger = get_logger(__name__)


async def handle_greeting(event: GreetingEvent, db: Database) -> None:
    """Procesa un evento de saludo.

    Demuestra:
    - Validación de invariantes de dominio → NonRetryableError
    - Persistencia con retry transparente (db.execute ya tiene retry)
    """
    if not event.message.strip():
        raise NonRetryableError(
            "Mensaje vacío después de strip",
            context={"event_id": event.event_id, "user_id": event.user_id},
        )

    try:
        await db.execute(
            "INSERT IGNORE INTO greetings (event_id, user_id, message) VALUES (%s, %s, %s)",
            event.event_id, event.user_id, event.message,
        )
    except Exception as exc:
        # Si llegó acá después del retry interno de db.execute, es algo serio.
        # Convertir a RetryableError para que el BaseConsumer le dé otra chance.
        raise RetryableError(
            "Failed to persist greeting",
            context={"event_id": event.event_id},
        ) from exc

    logger.info("greeting_processed", user_id=event.user_id, message=event.message)


async def handle_farewell(event: FarewellEvent, db: Database) -> None:
    """Procesa un evento de despedida."""
    await db.execute(
        "INSERT IGNORE INTO farewells (event_id, user_id, reason) VALUES (%s, %s, %s)",
        event.event_id, event.user_id, event.reason,
    )
    logger.info("farewell_processed", user_id=event.user_id, reason=event.reason)
