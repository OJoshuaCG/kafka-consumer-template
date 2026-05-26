"""Métricas Prometheus base — todos los consumers heredan estas.

Cada consumer puede agregar sus propias en `src/consumers/<name>/metrics.py`.
La exposición HTTP se hace con `prometheus_client.start_http_server(port)`
desde `consumer.run()` — una línea, sin FastAPI.
"""

from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram, start_http_server

# Mensajes procesados — label `status` ∈ {success, retry, dlq, duplicate}
MESSAGES_TOTAL = Counter(
    "kafka_messages_total",
    "Total de mensajes procesados",
    labelnames=("consumer", "topic", "status"),
)

# Duración del procesamiento por mensaje
MESSAGE_DURATION = Histogram(
    "kafka_message_duration_seconds",
    "Duración del procesamiento de un mensaje",
    labelnames=("consumer", "topic"),
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0),
)

# Mensajes enviados a DLQ
DLQ_TOTAL = Counter(
    "kafka_dlq_total",
    "Total de mensajes enviados a DLQ",
    labelnames=("consumer", "topic", "reason"),
)

# Retries acumulados
RETRY_TOTAL = Counter(
    "kafka_retry_total",
    "Total de retries por mensaje",
    labelnames=("consumer", "topic"),
)

# Eventos duplicados detectados por la capa de idempotencia
IDEMPOTENCY_DUPLICATES = Counter(
    "kafka_idempotency_duplicates_total",
    "Eventos detectados como duplicados (idempotencia)",
    labelnames=("consumer", "topic"),
)

# Estado del consumer (0=stopped, 1=starting, 2=running, 3=stopping, 4=error)
CONSUMER_STATE = Gauge(
    "kafka_consumer_state",
    "Estado del consumer (enum ConsumerState)",
    labelnames=("consumer",),
)

# Background tasks pendientes (para consumers que usan el pattern async)
BACKGROUND_TASKS_PENDING = Gauge(
    "kafka_background_tasks_pending",
    "Background tasks aún en vuelo",
    labelnames=("consumer",),
)


def start_metrics_server(port: int = 9090) -> None:
    """Levanta el endpoint Prometheus `/metrics` en `port`."""
    start_http_server(port)
