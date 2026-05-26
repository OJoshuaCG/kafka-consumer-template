# Manejo de errores

## Filosofía: una clase genérica, no cientos de clases específicas

El proyecto tiene exactamente **3 clases de excepción**:

```
DomainError           ← Base genérica y parametrizable
├── RetryableError    ← Transitorio: la DB está caída, timeout, 503 upstream
└── NonRetryableError ← Permanente: evento malformado, FK que no existe
```

No hay `UserNotFoundError`, `InvalidSignatureError`, `DBTimeoutError`,
`SchemaValidationError`, etc. Ese patrón (una clase por caso) produce:

- Cientos de clases que nadie lee
- `except` con listas interminables de tipos
- Tests que dependen de jerarquías de herencia frágiles
- La información útil (qué pasó) enterrada en el nombre de la clase en vez
  de en los datos del error

Con la filosofía del template, la información va en `context` y `message`:

```python
# ❌ Anti-patrón
raise UserNotFoundError(user_id)

# ✅ Correcto
raise NonRetryableError(
    "Usuario no encontrado",
    context={"user_id": user_id, "event_id": event_id},
)
```

---

## DomainError — cómo funciona

```python
from src.core.exceptions import RetryableError, NonRetryableError

# Captura automáticamente dónde se lanzó (file, function, line, code)
exc = NonRetryableError(
    "Total del pedido es negativo",
    context={"order_id": "ord-001", "total": -50.0},
    customer_tier="gold",   # cualquier kwarg extra
)

exc.message    # "Total del pedido es negativo"
exc.context    # {"order_id": "ord-001", "total": -50.0}
exc.extra      # {"customer_tier": "gold"}
exc.loc        # {"file": "src/consumers/...", "function": "handle_order", "line": 42, "code": "raise NonRetryableError(...)"}

exc.to_log_fields()
# {
#   "error_message": "Total del pedido es negativo",
#   "error_context": {"order_id": "ord-001", "total": -50.0},
#   "error_extra": {"customer_tier": "gold"},
#   "error_file": "src/consumers/mi_consumer/handlers.py",
#   "error_function": "handle_order",
#   "error_line": 42,
#   "error_code": 'raise NonRetryableError(...)'
# }
```

El `loc` se captura automáticamente con `inspect.stack()[2]` — el frame del
caller, no del `__init__`. No hay que pasarlo manualmente.

---

## Cuándo lanzar cada error

### RetryableError

Situaciones transitorias que pueden resolverse solas con el tiempo:

```python
# Red / DB
raise RetryableError("DB connection lost", context={"dsn_host": host})
raise RetryableError("Redis timeout", context={"key": key, "elapsed_ms": elapsed})

# Upstream API
raise RetryableError("Upstream 503", context={"service": "payments", "status": 503})

# Contención de recursos
raise RetryableError("Deadlock detectado", context={"table": "orders"})
```

El BaseConsumer va a reintentar con backoff exponencial + jitter. Si supera
`max_retries` (default: 3), manda a DLQ.

### NonRetryableError

Situaciones permanentes donde reintentar no ayuda:

```python
# Validación de dominio
if event.total < 0:
    raise NonRetryableError("Total negativo", context={"total": event.total})

# Referencia que no existe (FK)
if not await db.fetch_one("SELECT 1 FROM users WHERE id=$1", event.user_id):
    raise NonRetryableError("Usuario no existe", context={"user_id": event.user_id})

# Evento con estado inválido para la operación
if event.status != "pending":
    raise NonRetryableError(
        "Solo se procesan pedidos pending",
        context={"order_id": event.order_id, "status": event.status},
    )
```

El BaseConsumer manda a DLQ inmediatamente, sin ningún retry.

### Dejar propagar (Exception genérica)

Si la excepción no es de dominio (error de programación, ImportError, etc.),
dejarla propagar. El BaseConsumer la atrapa como "unhandled_exception", loggea
el stack trace completo, y manda a DLQ.

```python
# No atrapar esto en el handler:
# asyncpg.exceptions.UndefinedTableError  → error de programación
# pydantic.ValidationError                → schema mal definido
# AttributeError                          → bug en el código
```

---

## DLQ (Dead Letter Queue)

El DLQ es un topic Kafka separado donde van los mensajes que no pudieron
procesarse. El consumer principal siempre avanza — nunca se bloquea.

### Cuándo va un mensaje al DLQ

| Causa | Acción |
|---|---|
| JSON inválido (no parseable) | DLQ inmediato |
| `NonRetryableError` en el handler | DLQ inmediato |
| `RetryableError` que supera `max_retries` | DLQ después de N intentos |
| `Exception` no clasificada | DLQ inmediato + stack trace en logs |

### Headers del mensaje en DLQ

El BaseConsumer agrega headers al mensaje en el DLQ para facilitar el debugging:

```
x-dlq-reason            "non_retryable: Usuario no existe"
x-dlq-source-topic      "mi-consumer-events"
x-dlq-source-partition  "2"
x-dlq-source-offset     "15834"
```

Más todos los headers originales del mensaje.

### Ver mensajes en DLQ

Con Redpanda Console (`http://localhost:8080`), buscar el topic `*-dlq`.
Los headers están visibles por mensaje.

Con CLI:

```bash
# Listar últimos 10 mensajes del DLQ
docker exec -it redpanda \
  rpk topic consume example-events-dlq --num 10
```

### Reprocesar mensajes del DLQ

No hay un mecanismo automático. El proceso típico es:

1. Diagnosticar la causa raíz con los headers y el payload.
2. Corregir el bug o el dato.
3. Re-publicar el mensaje en el topic original usando el producer demo o un script.

```bash
# Ejemplo: re-publicar con rpk
rpk topic produce example-events <<< '{"type":"greeting","event_id":"evt-reprocess-001","user_id":"u1","message":"retry"}'
```

---

## Retry — mecánica interna

El retry usa **backoff exponencial con jitter**:

```
delay = min(base * 2^attempt * (0.5 + random(0,1)), cap)
```

Con `base=1.0s`, `cap=60.0s`:

| Intento | Delay mínimo | Delay máximo |
|---|---|---|
| 0 | 0.5s | 1.5s |
| 1 | 1.0s | 3.0s |
| 2 | 2.0s | 6.0s |
| 3 | 4.0s | 12.0s |
| 4 | 8.0s | 24.0s |
| ... | ... | máx 60s |

El **jitter** (factor aleatorio 0.5..1.5) evita que todos los pods reintentan
al mismo tiempo cuando hay un fallo de infra, lo que apagaría la DB o el upstream
que ya está estresado.

### Configurar retry por consumer

```python
class MiConsumer(BaseConsumer):
    name = "mi-consumer"
    max_retries = 5          # intentos antes de DLQ (default: 3)
    retry_base_delay = 0.5   # delay base en segundos (default: 1.0)
    retry_cap_delay = 30.0   # tope máximo (default: 60.0)
```

---

## Qué NO hacer

```python
# ❌ No crear clases de error por caso de dominio
class UserNotFoundError(Exception): pass
class InvalidTotalError(Exception): pass

# ❌ No atrapar RetryableError en el handler
try:
    await db.execute(...)
except RetryableError:
    ...  # el BaseConsumer ya lo hace

# ❌ No hacer commit manual en el handler
await self._consumer.commit()  # rompería el at-least-once delivery

# ❌ No silenciar excepciones
try:
    await risky_operation()
except Exception:
    pass  # el BaseConsumer nunca sabrá que falló
```
