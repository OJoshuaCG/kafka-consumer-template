"""BaseConsumer abstracto — el corazón del template.

Garantías del loop, en orden, por mensaje:

1. Setea ContextVars (`message_id`, `consumer_name`, `topic`, `event_type`, `attempt`).
2. Chequea idempotencia (Redis SET NX por event_id). Si duplicado: commit y next.
3. Try/except centralizado:
   - RetryableError → backoff con jitter + on_message_retry(); si supera max_retries → DLQ.
   - NonRetryableError → DLQ inmediato.
   - Exception no clasificada → DLQ + log con stack.
4. Commit manual del offset SOLO si process_message retornó sin excepción.
5. Métricas Prometheus actualizadas.

El handler NUNCA ve un duplicado, NUNCA decide retry, NUNCA commitea.
Es función pura: event → side effects.

Para trabajo > 30s, overridear `process_message_background()` en vez de
`process_message()`. Ver `docs/patterns/background-tasks.md`.
"""

from __future__ import annotations

import asyncio
import signal
import uuid
from collections.abc import Awaitable, Callable
from enum import IntEnum
from typing import TYPE_CHECKING, Any

import orjson

from src.core.context import (
    current_attempt,
    current_consumer_name,
    current_event_type,
    current_message_id,
    current_topic,
)
from src.core.exceptions import DomainError, NonRetryableError, RetryableError
from src.core.health import HealthCheckWriter
from src.core.idempotency import IdempotencyStore
from src.core.logging import get_logger
from src.core.metrics import (
    BACKGROUND_TASKS_PENDING,
    CONSUMER_STATE,
    DLQ_TOTAL,
    IDEMPOTENCY_DUPLICATES,
    MESSAGE_DURATION,
    MESSAGES_TOTAL,
    RETRY_TOTAL,
)
from src.core.retry import backoff_with_jitter

if TYPE_CHECKING:
    from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, ConsumerRecord
    from redis.asyncio import Redis

    from src.core.client import KafkaClientFactory


class ConsumerState(IntEnum):
    STOPPED = 0
    STARTING = 1
    RUNNING = 2
    STOPPING = 3
    ERROR = 4


logger = get_logger(__name__)


class BaseConsumer:
    """Clase base "abstracta por convención" para todos los consumers.

    No usa `ABC` porque la abstracción es por contrato: una subclase DEBE
    overridear `process_message()` o `process_message_background()`, pero
    NO ambos. `@abstractmethod` no expresa ese OR exclusivo.

    Cada consumer concreto:
    - Define `name`, `topic`, `group_id`, `dlq_topic`.
    - Implementa `process_message(event, raw_message)` (sync pattern), o
      `process_message_background(event, raw_message)` (async pattern).
    - Opcionalmente overridea `on_start()`, `on_stop()`, `on_message_retry()`.

    Dependencias inyectadas por constructor (DI):
        kafka_client_factory, redis, idempotency_namespace, dlq_topic,
        max_retries, retry_base_delay, health_path.
    """

    # ---- Configuración (subclase puede sobreescribir) ----
    name: str = "base-consumer"
    max_retries: int = 3
    retry_base_delay: float = 1.0
    retry_cap_delay: float = 60.0
    poll_timeout_ms: int = 1000

    def __init__(
        self,
        *,
        topic: str,
        group_id: str,
        dlq_topic: str,
        kafka_client_factory: KafkaClientFactory,
        redis: Redis,
        idempotency_namespace: str | None = None,
        idempotency_ttl_seconds: int = 7 * 24 * 3600,
        health_path: str = "/tmp/healthcheck",
        health_interval_seconds: float = 10.0,
        consumer_kwargs: dict[str, Any] | None = None,
    ) -> None:
        self._topic = topic
        self._group_id = group_id
        self._dlq_topic = dlq_topic
        self._kafka_factory = kafka_client_factory
        self._redis = redis
        self._idempotency = IdempotencyStore(
            redis,
            namespace=idempotency_namespace or self.name,
            ttl_seconds=idempotency_ttl_seconds,
        )
        self._health = HealthCheckWriter(health_path, interval_seconds=health_interval_seconds)
        self._consumer_kwargs = consumer_kwargs or {}

        self._consumer: AIOKafkaConsumer | None = None
        self._producer: AIOKafkaProducer | None = None
        self._state = ConsumerState.STOPPED
        self._stop_event = asyncio.Event()
        self._background_tasks: set[asyncio.Task[None]] = set()

        CONSUMER_STATE.labels(consumer=self.name).set(self._state)

    # =============================================================
    # Estado público
    # =============================================================
    @property
    def state(self) -> ConsumerState:
        return self._state

    def _set_state(self, state: ConsumerState) -> None:
        self._state = state
        CONSUMER_STATE.labels(consumer=self.name).set(state)

    # =============================================================
    # Hooks — overridear en subclase si hace falta
    # =============================================================
    async def on_start(self) -> None:
        """Llamado después de conectar a Kafka, antes del primer poll."""

    async def on_stop(self) -> None:
        """Llamado durante shutdown, antes de cerrar conexiones."""

    async def on_message_retry(self, event: dict[str, Any], attempt: int) -> None:
        """Llamado antes de cada retry de un mensaje. Útil para logging extra."""

    # =============================================================
    # Procesamiento — DOS patterns: sync o async (overridear UNO)
    # =============================================================
    async def process_message(self, event: dict[str, Any], raw_message: ConsumerRecord) -> None:
        """SYNC pattern: procesar y retornar antes de que se commitee el offset.

        Default: NotImplementedError. Overridear si el consumer hace trabajo < 30s.
        """
        raise NotImplementedError("Override process_message() OR process_message_background()")

    async def process_message_background(
        self,
        event: dict[str, Any],
        raw_message: ConsumerRecord,
    ) -> None:
        """ASYNC pattern: spawnear background task, commitear offset inmediato.

        Default: NotImplementedError. Overridear si el consumer hace trabajo > 30s.
        El task DEBE persistir status='processing' antes y status='done'/'failed' después.
        Ver `docs/patterns/background-tasks.md`.
        """
        raise NotImplementedError

    def _uses_background_pattern(self) -> bool:
        """Detecta si la subclase overrideo `process_message_background`."""
        return (
            type(self).process_message_background
            is not BaseConsumer.process_message_background
        )

    # =============================================================
    # Loop principal
    # =============================================================
    async def run(self) -> None:
        """Punto de entrada — corre hasta SIGINT/SIGTERM."""
        self._set_state(ConsumerState.STARTING)
        self._install_signal_handlers()

        self._consumer = self._kafka_factory.consumer(
            self._topic, group_id=self._group_id, **self._consumer_kwargs,
        )
        self._producer = self._kafka_factory.producer()

        await self._consumer.start()
        await self._producer.start()
        await self._health.start()

        try:
            await self.on_start()
            self._set_state(ConsumerState.RUNNING)
            logger.info(
                "consumer_started",
                consumer=self.name,
                topic=self._topic,
                group_id=self._group_id,
                pattern="background" if self._uses_background_pattern() else "sync",
            )
            await self._loop()
        except Exception:
            self._set_state(ConsumerState.ERROR)
            logger.exception("consumer_crashed", consumer=self.name)
            raise
        finally:
            await self._shutdown()

    async def _loop(self) -> None:
        assert self._consumer is not None
        while not self._stop_event.is_set():
            try:
                batch = await asyncio.wait_for(
                    self._consumer.getmany(timeout_ms=self.poll_timeout_ms),
                    timeout=(self.poll_timeout_ms / 1000.0) + 1.0,
                )
            except TimeoutError:
                continue

            for _tp, messages in batch.items():
                for raw_message in messages:
                    if self._stop_event.is_set():
                        return
                    await self._dispatch(raw_message)

    async def _dispatch(self, raw_message: ConsumerRecord) -> None:
        """Procesa UN mensaje aplicando todas las garantías del loop."""
        event, parse_error = self._parse(raw_message)
        message_id = self._extract_message_id(event, raw_message)
        event_type = (event or {}).get("type") or (event or {}).get("event_type")

        # ContextVars — todo log debajo en el stack hereda esto
        token_msg = current_message_id.set(message_id)
        token_consumer = current_consumer_name.set(self.name)
        token_topic = current_topic.set(raw_message.topic)
        token_type = current_event_type.set(event_type)
        token_attempt = current_attempt.set(0)

        try:
            if parse_error is not None:
                # Evento no parseable → DLQ directo. No tiene sentido reintentar.
                await self._send_to_dlq(raw_message, reason=f"parse_error: {parse_error}")
                await self._commit(raw_message)
                return

            assert event is not None  # parse_error es None ⇒ event existe

            # Idempotencia — chequea ANTES de invocar el handler.
            event_id = self._extract_event_id(event, message_id)
            if not await self._idempotency.claim(event_id):
                IDEMPOTENCY_DUPLICATES.labels(consumer=self.name, topic=raw_message.topic).inc()
                MESSAGES_TOTAL.labels(
                    consumer=self.name, topic=raw_message.topic, status="duplicate",
                ).inc()
                logger.info("event_skipped_duplicate", event_id=event_id)
                await self._commit(raw_message)
                return

            # Procesar con retry loop
            await self._process_with_retry(event, raw_message)

        finally:
            current_message_id.reset(token_msg)
            current_consumer_name.reset(token_consumer)
            current_topic.reset(token_topic)
            current_event_type.reset(token_type)
            current_attempt.reset(token_attempt)

    async def _process_with_retry(
        self,
        event: dict[str, Any],
        raw_message: ConsumerRecord,
    ) -> None:
        for attempt in range(self.max_retries + 1):
            current_attempt.set(attempt)
            try:
                with MESSAGE_DURATION.labels(
                    consumer=self.name, topic=raw_message.topic,
                ).time():
                    if self._uses_background_pattern():
                        await self._spawn_background(event, raw_message)
                    else:
                        await self.process_message(event, raw_message)

                MESSAGES_TOTAL.labels(
                    consumer=self.name, topic=raw_message.topic, status="success",
                ).inc()
                if not self._uses_background_pattern():
                    await self._commit(raw_message)
                return

            except RetryableError as exc:
                MESSAGES_TOTAL.labels(
                    consumer=self.name, topic=raw_message.topic, status="retry",
                ).inc()
                if attempt >= self.max_retries:
                    logger.error(
                        "max_retries_exceeded",
                        attempt=attempt,
                        **exc.to_log_fields(),
                    )
                    await self._send_to_dlq(raw_message, reason=f"max_retries: {exc.message}")
                    await self._commit(raw_message)
                    return
                RETRY_TOTAL.labels(consumer=self.name, topic=raw_message.topic).inc()
                delay = backoff_with_jitter(attempt, self.retry_base_delay, self.retry_cap_delay)
                logger.warning(
                    "retrying_message",
                    attempt=attempt,
                    next_delay_seconds=delay,
                    **exc.to_log_fields(),
                )
                await self.on_message_retry(event, attempt)
                await asyncio.sleep(delay)

            except NonRetryableError as exc:
                logger.error("non_retryable_error", **exc.to_log_fields())
                await self._send_to_dlq(raw_message, reason=f"non_retryable: {exc.message}")
                await self._commit(raw_message)
                return

            except Exception:
                logger.exception("unhandled_exception_in_handler")
                await self._send_to_dlq(raw_message, reason="unhandled_exception")
                await self._commit(raw_message)
                return

    # =============================================================
    # Background tasks (opt-in)
    # =============================================================
    async def _spawn_background(
        self,
        event: dict[str, Any],
        raw_message: ConsumerRecord,
    ) -> None:
        """Spawnea el trabajo como task, commitea offset inmediato.

        IMPORTANTE: la subclase debe persistir `status='processing'` en
        `process_message_background()` ANTES de hacer trabajo I/O, para que
        crash recovery pueda retomarlo.
        """
        task = asyncio.create_task(
            self.process_message_background(event, raw_message),
            name=f"{self.name}-bg-{uuid.uuid4().hex[:8]}",
        )
        self._background_tasks.add(task)
        task.add_done_callback(self._on_background_done)
        BACKGROUND_TASKS_PENDING.labels(consumer=self.name).set(len(self._background_tasks))
        await self._commit(raw_message)

    def _on_background_done(self, task: asyncio.Task[None]) -> None:
        self._background_tasks.discard(task)
        BACKGROUND_TASKS_PENDING.labels(consumer=self.name).set(len(self._background_tasks))
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            logger.error("background_task_failed", exc_info=exc, task_name=task.get_name())

    # =============================================================
    # Helpers
    # =============================================================
    def _parse(self, raw_message: ConsumerRecord) -> tuple[dict[str, Any] | None, str | None]:
        if raw_message.value is None:
            return None, "empty_payload"
        try:
            return orjson.loads(raw_message.value), None
        except orjson.JSONDecodeError as exc:
            return None, f"json_decode_error: {exc}"

    def _extract_message_id(
        self,
        event: dict[str, Any] | None,
        raw_message: ConsumerRecord,
    ) -> str:
        if event and (mid := event.get("message_id") or event.get("id")):
            return str(mid)
        # Si el evento no trae id, usar (topic, partition, offset) como id sintético.
        return f"{raw_message.topic}-{raw_message.partition}-{raw_message.offset}"

    def _extract_event_id(self, event: dict[str, Any], fallback: str) -> str:
        """Para idempotencia: usar event_id explícito si existe, sino el message_id."""
        return str(event.get("event_id") or event.get("id") or fallback)

    async def _commit(self, raw_message: ConsumerRecord) -> None:
        assert self._consumer is not None
        from aiokafka import TopicPartition

        tp = TopicPartition(raw_message.topic, raw_message.partition)
        await self._consumer.commit({tp: raw_message.offset + 1})

    async def _send_to_dlq(self, raw_message: ConsumerRecord, *, reason: str) -> None:
        assert self._producer is not None
        try:
            headers = list(raw_message.headers or [])
            headers.append(("x-dlq-reason", reason.encode()))
            headers.append(("x-dlq-source-topic", raw_message.topic.encode()))
            headers.append(("x-dlq-source-partition", str(raw_message.partition).encode()))
            headers.append(("x-dlq-source-offset", str(raw_message.offset).encode()))
            await self._producer.send_and_wait(
                self._dlq_topic,
                value=raw_message.value,
                key=raw_message.key,
                headers=headers,
            )
            DLQ_TOTAL.labels(
                consumer=self.name, topic=raw_message.topic, reason=reason.split(":")[0],
            ).inc()
            MESSAGES_TOTAL.labels(
                consumer=self.name, topic=raw_message.topic, status="dlq",
            ).inc()
            logger.warning("sent_to_dlq", reason=reason, dlq_topic=self._dlq_topic)
        except Exception:
            logger.exception("dlq_send_failed", reason=reason)
            raise

    # =============================================================
    # Shutdown
    # =============================================================
    def _install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self._stop_event.set)
            except NotImplementedError:
                # add_signal_handler no está en Windows
                signal.signal(sig, lambda _s, _f: self._stop_event.set())

    async def _shutdown(self) -> None:
        self._set_state(ConsumerState.STOPPING)
        logger.info("consumer_stopping", consumer=self.name)
        try:
            await self.on_stop()
        except Exception:
            logger.exception("on_stop_failed")

        # Esperar background tasks pendientes
        if self._background_tasks:
            logger.info("waiting_background_tasks", pending=len(self._background_tasks))
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self._background_tasks, return_exceptions=True),
                    timeout=30.0,
                )
            except TimeoutError:
                logger.warning("background_tasks_timeout", pending=len(self._background_tasks))

        if self._consumer is not None:
            await self._consumer.stop()
        if self._producer is not None:
            await self._producer.stop()
        await self._health.stop()
        self._set_state(ConsumerState.STOPPED)
        logger.info("consumer_stopped", consumer=self.name)


# ===================================================================
# Helper para crear un entry point `run()` por consumer en pyproject.toml
# ===================================================================
def make_run_function(
    consumer_factory: Callable[[], Awaitable[BaseConsumer]],
) -> Callable[[], None]:
    """Crea una función sincrónica que arranca el event loop y corre el consumer.

    Ejemplo de uso en `src/consumers/example/consumer.py`:

        async def _build() -> ExampleConsumer:
            ...

        run = make_run_function(_build)

    Y en pyproject.toml: `example-consumer = "src.consumers.example.consumer:run"`.
    """

    def run() -> None:
        async def _main() -> None:
            consumer = await consumer_factory()
            await consumer.run()

        asyncio.run(_main())

    return run


# Re-export para conveniencia
__all__ = [
    "BaseConsumer",
    "ConsumerState",
    "DomainError",
    "make_run_function",
]
