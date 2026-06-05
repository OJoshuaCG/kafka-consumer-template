"""ExampleConsumer — usar como template para crear nuevos consumers.

Workflow para crear uno nuevo:
1. `cp -r src/consumers/example/ src/consumers/mi_consumer/`
2. Cambiar `env_prefix` en `settings.py`
3. Reemplazar `schemas.py` con tus Pydantic models
4. Reemplazar `handlers.py` con tus funciones async
5. Adaptar este archivo (nombre, dispatch, on_start)
6. Registrar entry point en `pyproject.toml`:
   `mi-consumer = "src.consumers.mi_consumer.consumer:run"`
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pydantic import TypeAdapter, ValidationError

from src.config import get_global_settings
from src.consumers.example.handlers import handle_farewell, handle_greeting
from src.consumers.example.metrics import FAREWELLS_PROCESSED, GREETINGS_PROCESSED
from src.consumers.example.schemas import ExampleEvent, FarewellEvent, GreetingEvent
from src.consumers.example.settings import get_example_settings
from src.core import BaseConsumer
from src.core.client import KafkaClientFactory
from src.core.exceptions import NonRetryableError
from src.core.logging import get_logger, setup_logging
from src.core.metrics import start_metrics_server
from src.core.redis import RedisClientFactory
from src.db.database import Database, create_database

if TYPE_CHECKING:
    from aiokafka import ConsumerRecord

logger = get_logger(__name__)

_event_adapter: TypeAdapter[ExampleEvent] = TypeAdapter(ExampleEvent)


class ExampleConsumer(BaseConsumer):
    """Consumer del topic `example-events`.

    Procesamiento SYNC (sin background tasks) — handlers son rápidos.
    """

    name = "example-consumer"

    def __init__(self, *, db: Database, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._db = db

    async def on_start(self) -> None:
        await self._db.connect()

    async def on_stop(self) -> None:
        await self._db.close()

    async def process_message(
        self,
        event: dict[str, Any],
        _raw_message: ConsumerRecord,
    ) -> None:
        try:
            parsed = _event_adapter.validate_python(event)
        except ValidationError as exc:
            raise NonRetryableError(
                "Event failed schema validation",
                context={"errors": exc.errors(), "raw_event": event},
            ) from exc

        if isinstance(parsed, GreetingEvent):
            await handle_greeting(parsed, self._db)
            GREETINGS_PROCESSED.inc()
        elif isinstance(parsed, FarewellEvent):
            await handle_farewell(parsed, self._db)
            FAREWELLS_PROCESSED.inc()


async def _build() -> ExampleConsumer:
    """Compone el consumer con todas sus dependencias."""
    global_settings = get_global_settings()
    example_settings = get_example_settings()

    setup_logging(level=global_settings.log_level, environment=global_settings.environment)

    if global_settings.metrics_enabled:
        start_metrics_server(global_settings.metrics_port)

    kafka_factory = KafkaClientFactory(
        bootstrap_servers=global_settings.kafka_bootstrap_servers,
        security_protocol=global_settings.kafka_security_protocol,
        sasl_username=global_settings.kafka_sasl_username,
        sasl_password=global_settings.kafka_sasl_password,
    )
    redis_factory = RedisClientFactory()
    redis = redis_factory.client(global_settings.redis_url)
    db = create_database(global_settings.database_url)

    return ExampleConsumer(
        db=db,
        topic=example_settings.topic,
        group_id=example_settings.group_id,
        dlq_topic=example_settings.dlq_topic,
        kafka_client_factory=kafka_factory,
        redis=redis,
        health_path=global_settings.health_file_path,
        health_interval_seconds=global_settings.health_write_interval_seconds,
    )


def run() -> None:
    """Entry point — registrado en pyproject.toml como `example-consumer`."""
    import asyncio

    async def _main() -> None:
        consumer = await _build()
        await consumer.run()

    asyncio.run(_main())


if __name__ == "__main__":
    run()
