# Observabilidad

## Logging estructurado

El proyecto usa **structlog** con `orjson` como backend de serialización.
En producción, cada log es una línea JSON. En desarrollo, salida colorida legible.

### Formato en producción (`ENVIRONMENT != development`)

```json
{"timestamp": "2024-11-15T10:30:45.123Z", "level": "info", "event": "consumer_started", "environment": "production", "consumer": "example-consumer", "topic": "example-events", "group_id": "example-consumer", "pattern": "sync"}
```

Los campos se ordenan: `timestamp → level → event → environment → resto`.
Esto facilita el escaneo visual en Datadog/Loki donde las primeras columnas
son las más importantes.

### Formato en desarrollo (`ENVIRONMENT=development`)

```
2024-11-15 10:30:45 [info     ] consumer_started [example_consumer] consumer=example-consumer topic=example-events
```

### Obtener un logger

```python
from src.core.logging import get_logger

logger = get_logger(__name__)   # convención: uno por módulo

# Uso:
logger.info("order_processed", order_id=order.id, total=order.total)
logger.warning("retrying_upstream", attempt=2, service="payments")
logger.error("non_retryable_error", **exc.to_log_fields())
logger.exception("unexpected_crash")  # incluye el stack trace
```

**Convención de nombres de eventos**: `snake_case`, verbo en pasado o gerundio.
Ejemplos: `order_processed`, `retrying_message`, `consumer_started`, `dlq_sent`.

### ContextVars automáticos

Mientras el BaseConsumer procesa un mensaje, estos campos se inyectan en
**todos los logs** del stack, sin pasarlos por parámetro:

| Campo | Ejemplo |
|---|---|
| `message_id` | `"evt-001"` |
| `consumer_name` | `"example-consumer"` |
| `topic` | `"example-events"` |
| `event_type` | `"greeting"` |
| `attempt` | `2` (0-indexed) |

Ejemplo de log generado por un handler:

```json
{
  "timestamp": "2024-11-15T10:30:45Z",
  "level": "info",
  "event": "greeting_processed",
  "environment": "production",
  "message_id": "evt-abc123",
  "consumer_name": "example-consumer",
  "topic": "example-events",
  "event_type": "greeting",
  "attempt": 0,
  "user_id": "u-001",
  "greeting_id": 42
}
```

---

## Métricas Prometheus

El consumer expone un endpoint `/metrics` en el puerto `METRICS_PORT` (default: 9090).
No se usa FastAPI ni ningún HTTP server extra — solo `prometheus_client.start_http_server()`.

### Métricas base (todos los consumers)

#### `kafka_messages_total` (Counter)

Total de mensajes procesados por resultado.

```
kafka_messages_total{consumer="example-consumer", topic="example-events", status="success"} 1523
kafka_messages_total{consumer="example-consumer", topic="example-events", status="retry"} 12
kafka_messages_total{consumer="example-consumer", topic="example-events", status="dlq"} 3
kafka_messages_total{consumer="example-consumer", topic="example-events", status="duplicate"} 47
```

**Alerta sugerida**: `rate(kafka_messages_total{status="dlq"}[5m]) > 0.1`

#### `kafka_message_duration_seconds` (Histogram)

Duración del procesamiento por mensaje. Buckets: 10ms a 120s.

```
kafka_message_duration_seconds_bucket{consumer="example-consumer", topic="example-events", le="0.1"} 1490
kafka_message_duration_seconds_bucket{consumer="example-consumer", topic="example-events", le="1.0"} 1520
kafka_message_duration_seconds_p99  →  ver en Grafana
```

**Alerta sugerida**: `histogram_quantile(0.99, rate(kafka_message_duration_seconds_bucket[5m])) > 5`

#### `kafka_dlq_total` (Counter)

Mensajes que fueron a DLQ, por razón.

```
kafka_dlq_total{consumer="example-consumer", topic="example-events", reason="non_retryable"} 3
kafka_dlq_total{consumer="example-consumer", topic="example-events", reason="max_retries"} 1
kafka_dlq_total{consumer="example-consumer", topic="example-events", reason="parse_error"} 0
```

#### `kafka_retry_total` (Counter)

Reintentos acumulados.

```
kafka_retry_total{consumer="example-consumer", topic="example-events"} 12
```

#### `kafka_idempotency_duplicates_total` (Counter)

Eventos detectados como duplicados antes de llegar al handler.

```
kafka_idempotency_duplicates_total{consumer="example-consumer", topic="example-events"} 47
```

Un número elevado puede indicar rebalances frecuentes o re-procesamiento masivo.

#### `kafka_consumer_state` (Gauge)

Estado actual del consumer como número (0=STOPPED, 1=STARTING, 2=RUNNING, 3=STOPPING, 4=ERROR).

```
kafka_consumer_state{consumer="example-consumer"} 2
```

**Alerta sugerida**: `kafka_consumer_state{} == 4` (estado ERROR).

#### `kafka_background_tasks_pending` (Gauge)

Solo relevante si el consumer usa el [pattern de background tasks](patterns/background-tasks.md).

```
kafka_background_tasks_pending{consumer="bulk-consumer"} 5
```

Debe volver a 0 entre ráfagas. Si crece indefinidamente, hay un leak.

### Consultar métricas manualmente

```bash
# Ver todas las métricas Kafka
curl -s http://localhost:9090/metrics | grep ^kafka_

# Ver estado del consumer
curl -s http://localhost:9090/metrics | grep kafka_consumer_state

# Ver tasa de mensajes en los últimos 5s
watch -n1 'curl -s http://localhost:9090/metrics | grep kafka_messages_total'
```

### Agregar métricas específicas al consumer

```python
# src/consumers/mi_consumer/metrics.py
from prometheus_client import Counter, Histogram

ORDERS_INSERTED = Counter(
    "mi_consumer_orders_inserted_total",
    "Pedidos insertados correctamente",
    labelnames=("status",),
)

UPSTREAM_DURATION = Histogram(
    "mi_consumer_upstream_call_seconds",
    "Duración de llamadas al upstream de pagos",
    buckets=(0.1, 0.5, 1.0, 2.5, 5.0),
)
```

---

## Health check

El consumer escribe `time.time()` (timestamp Unix) al archivo `HEALTH_FILE_PATH`
(default: `/tmp/healthcheck`) cada `HEALTH_WRITE_INTERVAL_SECONDS` (default: 10s).

Si el consumer se cuelga, deja de escribir. K8s lo detecta y lo reinicia.

### K8s liveness probe

```yaml
livenessProbe:
  exec:
    command:
      - python
      - -c
      - |
        import os, sys, time
        path = "/tmp/healthcheck"
        if not os.path.exists(path):
            sys.exit(1)
        age = time.time() - os.path.getmtime(path)
        sys.exit(0 if age < 60 else 1)
  initialDelaySeconds: 15
  periodSeconds: 20
  failureThreshold: 3
```

El probe falla si el archivo tiene más de 60 segundos de antigüedad (6 ciclos
de escritura sin actualizarse).

### Verificar health manualmente

```bash
# Debe existir y tener un timestamp reciente
cat /tmp/healthcheck   # muestra algo como: 1731661845.123456

# Calcular antigüedad
python3 -c "import os,time; print(f'{time.time() - os.path.getmtime(\"/tmp/healthcheck\"):.1f}s ago')"
```

---

## Logs en producción — queries útiles

### Datadog

```
# Todos los errores de un consumer
@consumer:example-consumer status:error

# Mensajes que fueron a DLQ
@event:sent_to_dlq @consumer:example-consumer

# Tasa de retries
sum(count_over_time({@event:retrying_message @consumer:example-consumer}[5m]))

# Tracing de un mensaje específico por event_id
@message_id:evt-abc123
```

### Loki / Grafana

```
# Todos los logs del consumer
{app="example-consumer"}

# Solo errores
{app="example-consumer"} |= "error"

# Tracing de un mensaje
{app="example-consumer"} | json | message_id="evt-abc123"

# Mensajes en DLQ en la última hora
{app="example-consumer"} | json | event="sent_to_dlq" [1h]
```

---

## Nivel de log por ambiente

| Evento | Nivel |
|---|---|
| Consumer arrancado/parado | `info` |
| Mensaje procesado OK | `info` (solo en DEBUG para alto volumen) |
| Retry de un mensaje | `warning` |
| Mensaje a DLQ | `warning` |
| Max retries excedido | `error` |
| Excepción no clasificada en handler | `error` (con stack trace) |
| Consumer en estado ERROR | `error` |
| Background task falló | `error` |

En producción usar `LOG_LEVEL=INFO`. En desarrollo `LOG_LEVEL=DEBUG` para ver
cada mensaje procesado.
