# Testing

## Estrategia en tres niveles

```
tests/
├── unit/           ← pytest puro. Sin infra. Rápidos (< 5s total).
│   ├── core/               ← Pruebas del framework: retry, context, exceptions, utils
│   └── consumers/example/  ← Pruebas de handlers con dependencias falsas
└── integration/    ← Testcontainers. Con infra real. Más lentos (30s-120s).
    └── test_idempotency.py ← IdempotencyStore contra Redis real
```

| Nivel | Infra necesaria | Velocidad | Cuándo correr |
|---|---|---|---|
| Unit | Ninguna | < 5s | Siempre (pre-commit, CI) |
| Integration | Docker | 30-120s | Pull requests, CI completo |

La regla de oro: **los tests unitarios nunca mockean `aiokafka`**. Si necesitás
probar la interacción real con Kafka, es un test de integración con Testcontainers.

---

## Correr tests

```bash
# Solo unitarios (rápidos, sin Docker)
uv run pytest tests/unit/ -v

# Solo integración (necesita Docker corriendo)
uv run pytest tests/integration/ -v

# Todos
uv run pytest -v

# Con cobertura
uv run pytest tests/unit/ --cov=src --cov-report=term-missing

# Un archivo específico
uv run pytest tests/unit/core/test_retry.py -v

# Un test específico
uv run pytest tests/unit/core/test_retry.py::TestRetryAsync::test_reraises_after_max_attempts -v

# Fallar rápido al primer error
uv run pytest tests/unit/ -x

# Rerun solo los fallidos
uv run pytest tests/unit/ --lf
```

---

## Tests unitarios — cómo escribirlos

### Handlers (el caso más común)

Los handlers son funciones puras: reciben un evento y dependencias inyectadas,
hacen side effects. La dependencia más común es `Database`.

Patrón: crear una `FakeDB` que capture las llamadas y verifique lo que se insertó.

```python
# tests/unit/consumers/mi_consumer/test_handlers.py
import pytest
from src.consumers.mi_consumer.handlers import handle_order_received
from src.consumers.mi_consumer.schemas import OrderReceivedEvent


class FakeDB:
    """Captura llamadas a db.execute() para assertions."""
    def __init__(self) -> None:
        self.executed: list[tuple] = []

    async def execute(self, query: str, *args) -> str:
        self.executed.append((query, args))
        return "INSERT 0 1"

    async def fetch_one(self, query: str, *args):
        return None


@pytest.mark.asyncio
async def test_persists_order_on_valid_event():
    event = OrderReceivedEvent(
        type="order_received",
        event_id="evt-001",
        order_id="ord-001",
        total=99.99,
    )
    db = FakeDB()

    await handle_order_received(event, db)

    assert len(db.executed) == 1
    query, args = db.executed[0]
    assert "INSERT INTO orders" in query
    assert "ord-001" in args


@pytest.mark.asyncio
async def test_raises_non_retryable_on_negative_total():
    from src.core.exceptions import NonRetryableError

    event = OrderReceivedEvent(
        type="order_received",
        event_id="evt-002",
        order_id="ord-002",
        total=-1.0,
    )
    db = FakeDB()

    with pytest.raises(NonRetryableError, match="total negativo"):
        await handle_order_received(event, db)
```

### Retry y backoff

```python
# tests/unit/core/test_retry.py
import pytest
from src.core.retry import backoff_with_jitter, retry_async
from src.core.exceptions import RetryableError


def test_jitter_stays_within_bounds():
    for attempt in range(5):
        delay = backoff_with_jitter(attempt, base=1.0, cap=60.0)
        expected_min = 1.0 * (2 ** attempt) * 0.5
        expected_max = min(1.0 * (2 ** attempt) * 1.5, 60.0)
        assert expected_min <= delay <= expected_max


@pytest.mark.asyncio
async def test_retries_then_succeeds():
    call_count = 0

    async def flaky():
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise RetryableError("transient")
        return "ok"

    result = await retry_async(flaky, max_attempts=3, base_delay=0.0)
    assert result == "ok"
    assert call_count == 3
```

### Excepciones y captura de contexto

```python
# tests/unit/core/test_exceptions.py
from src.core.exceptions import NonRetryableError, RetryableError, DomainError


def test_captures_caller_location():
    exc = NonRetryableError("algo falló", context={"id": "x"})
    assert exc.loc["file"].endswith(".py")
    assert exc.loc["function"] == "test_captures_caller_location"
    assert isinstance(exc.loc["line"], int)


def test_to_log_fields_has_expected_keys():
    exc = RetryableError("timeout", context={"db": "primary"})
    fields = exc.to_log_fields()
    assert "error_message" in fields
    assert "error_context" in fields
    assert "error_file" in fields
    assert fields["error_context"] == {"db": "primary"}


def test_hierarchy():
    exc = RetryableError("transient")
    assert isinstance(exc, DomainError)
    assert isinstance(exc, Exception)
```

### ContextVars

```python
# tests/unit/core/test_context.py
import pytest
import asyncio
from src.core.context import (
    current_message_id, current_consumer_name, context_snapshot
)


def test_defaults_are_none():
    snap = context_snapshot()
    assert snap["message_id"] is None
    assert snap["consumer_name"] is None


def test_set_and_snapshot():
    token = current_message_id.set("msg-001")
    try:
        snap = context_snapshot()
        assert snap["message_id"] == "msg-001"
    finally:
        current_message_id.reset(token)


@pytest.mark.asyncio
async def test_isolation_across_tasks():
    """Cada task tiene su propio contexto."""
    results = {}

    async def task_a():
        token = current_message_id.set("task-a")
        await asyncio.sleep(0)  # yield
        results["a"] = context_snapshot()["message_id"]
        current_message_id.reset(token)

    async def task_b():
        token = current_message_id.set("task-b")
        await asyncio.sleep(0)  # yield
        results["b"] = context_snapshot()["message_id"]
        current_message_id.reset(token)

    await asyncio.gather(task_a(), task_b())
    assert results["a"] == "task-a"
    assert results["b"] == "task-b"
```

---

## Tests de integración — cómo escribirlos

Los tests de integración usan fixtures de `tests/integration/conftest.py` que
levantan Redpanda, Redis y Postgres **una sola vez por sesión** (scope="session").

### Fixtures disponibles

| Fixture | Tipo | Qué provee |
|---|---|---|
| `kafka_bootstrap` | `str` | Bootstrap server del Redpanda en el contenedor |
| `postgres_dsn` | `str` | DSN asyncpg del Postgres en el contenedor |
| `redis_url` | `str` | URL del Redis en el contenedor |
| `redis_client` | `Redis` | Cliente async con `flushdb()` al terminar |
| `unique_topic` | `str` | Topic con suffix random (aislamiento entre tests) |
| `unique_group_id` | `str` | Group ID con suffix random |

### Ejemplo: test de idempotencia contra Redis real

```python
# tests/integration/test_idempotency.py
import pytest
from src.core.idempotency import IdempotencyStore


@pytest.mark.asyncio
async def test_claim_returns_true_first_time(redis_client):
    store = IdempotencyStore(redis_client, namespace="test")
    assert await store.claim("evt-001") is True


@pytest.mark.asyncio
async def test_claim_returns_false_on_duplicate(redis_client):
    store = IdempotencyStore(redis_client, namespace="test")
    await store.claim("evt-dup")
    assert await store.claim("evt-dup") is False


@pytest.mark.asyncio
async def test_namespaces_are_isolated(redis_client):
    store_a = IdempotencyStore(redis_client, namespace="consumer-a")
    store_b = IdempotencyStore(redis_client, namespace="consumer-b")
    await store_a.claim("evt-001")
    # Mismo event_id en namespace distinto → nuevo claim
    assert await store_b.claim("evt-001") is True
```

### Ejemplo: test end-to-end de un consumer

```python
# tests/integration/consumers/mi_consumer/test_consumer.py
import asyncio
import json
import pytest
from aiokafka import AIOKafkaProducer
from src.core.client import KafkaClientFactory
from src.consumers.mi_consumer.consumer import MiConsumer
from src.db.database import Database


@pytest.mark.asyncio
async def test_consumer_processes_valid_event(
    kafka_bootstrap, redis_client, postgres_dsn, unique_topic, unique_group_id
):
    # Preparar infra
    db = Database(postgres_dsn)
    await db.connect()

    factory = KafkaClientFactory(bootstrap_servers=kafka_bootstrap)
    consumer = MiConsumer(
        topic=unique_topic,
        group_id=unique_group_id,
        dlq_topic=unique_topic + "-dlq",
        kafka_client_factory=factory,
        redis=redis_client,
        db=db,
    )

    # Publicar un evento de prueba
    producer = AIOKafkaProducer(bootstrap_servers=kafka_bootstrap)
    await producer.start()
    await producer.send_and_wait(
        unique_topic,
        json.dumps({
            "type": "order_received",
            "event_id": "evt-test-001",
            "order_id": "ord-001",
            "total": 50.0,
        }).encode(),
    )
    await producer.stop()

    # Correr el consumer por 3 segundos y parar
    async def run_briefly():
        await asyncio.wait_for(consumer.run(), timeout=3.0)

    try:
        await run_briefly()
    except TimeoutError:
        pass  # esperado

    # Verificar que el evento fue procesado
    row = await db.fetch_one(
        "SELECT * FROM orders WHERE order_id = $1", "ord-001"
    )
    assert row is not None

    await db.close()
```

---

## Convenciones de tests

### Nombrado

- Archivos: `test_<módulo>.py`
- Clases: `Test<ConceptoQueSePrueba>` (una clase por concepto)
- Métodos: `test_<qué_hace>_<cuándo>` o `test_<comportamiento_esperado>`

Ejemplos buenos:
- `test_claim_returns_false_on_duplicate`
- `test_jitter_stays_within_bounds`
- `test_raises_non_retryable_on_negative_total`

### Estructura AAA

```python
async def test_ejemplo():
    # Arrange — preparar datos y dependencias
    event = MiEvent(type="x", event_id="e1", ...)
    db = FakeDB()

    # Act — ejecutar lo que se prueba
    await handle_x(event, db)

    # Assert — verificar el resultado
    assert len(db.executed) == 1
```

### Aislamiento en integración

- Usar siempre `unique_topic` y `unique_group_id` para evitar interferencia entre tests.
- El fixture `redis_client` hace `flushdb()` al terminar — no limpiar manualmente.
- Para Postgres: crear y limpiar datos dentro del test, o usar transactions con rollback.

### Qué NO hacer en tests

```python
# ❌ No mockear aiokafka — si necesitás probar con Kafka, usar Testcontainers
from unittest.mock import MagicMock
consumer = MagicMock(spec=AIOKafkaConsumer)   # NO

# ❌ No importar FastAPI en tests
from fastapi.testclient import TestClient      # NO

# ❌ No hardcodear puertos o credenciales
DATABASE_URL = "postgresql://localhost:5432/db"  # NO — usar fixture postgres_dsn

# ✅ FakeDB para handlers
class FakeDB:
    async def execute(self, q, *args): ...

# ✅ Testcontainers para tests de integración
def test_algo(redis_client, unique_topic): ...
```

---

## CI — orden de ejecución recomendado

```yaml
# En tu pipeline CI:
- name: Lint
  run: uv run ruff check src/ tests/

- name: Type check
  run: uv run mypy src/

- name: Unit tests
  run: uv run pytest tests/unit/ -v --tb=short

- name: Integration tests
  run: uv run pytest tests/integration/ -v --tb=short
  # Requiere Docker en el runner
```

Los tests unitarios son rápidos y sin dependencias externas — deben pasar siempre.
Los de integración necesitan Docker y pueden tardar 1-2 minutos.
