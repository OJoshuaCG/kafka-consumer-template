# TODO — Pendientes del proyecto

Inventario completo de lo que falta para que el proyecto sea completamente
robusto en producción. Organizado por prioridad.

---

## Prioridad 1 — Bloqueantes para funcionar end-to-end

### 1.1 Migración inicial de Alembic

**Estado**: `alembic/versions/` está vacío.
Los handlers escriben en `greetings` y `farewells`, pero las tablas no existen
en la DB porque ninguna migración las crea.

**Qué hacer**:

1. Agregar modelos SQLAlchemy en `alembic/env.py` o en un archivo `src/db/models.py`
   para que `--autogenerate` detecte los cambios:

```python
# src/db/models.py
from sqlalchemy import Column, String, Text, DateTime, func
from sqlalchemy.orm import DeclarativeBase

class Base(DeclarativeBase):
    pass

class Greeting(Base):
    __tablename__ = "greetings"
    event_id = Column(String, primary_key=True)
    user_id  = Column(String, nullable=False)
    message  = Column(Text, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

class Farewell(Base):
    __tablename__ = "farewells"
    event_id = Column(String, primary_key=True)
    user_id  = Column(String, nullable=False)
    reason   = Column(Text)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
```

2. Conectar `target_metadata` en `alembic/env.py`:

```python
from src.db.models import Base
target_metadata = Base.metadata
```

3. Generar y aplicar:

```bash
uv run alembic revision --autogenerate -m "initial tables"
uv run alembic upgrade head
```

**Archivos**: `src/db/models.py` (nuevo), `alembic/env.py`, `alembic/versions/`

---

### 1.2 Deprecación `@app.on_event` en producer_demo

**Estado**: `tools/producer_demo/main.py` usa `@app.on_event("startup")` que
está deprecado desde FastAPI 0.93. Genera warnings en cada arranque.

**Qué hacer**: migrar a `lifespan` context manager:

```python
# tools/producer_demo/main.py
from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    global _producer
    _producer = AIOKafkaProducer(...)
    await _producer.start()
    yield
    if _producer:
        await _producer.stop()

app = FastAPI(lifespan=lifespan, ...)
```

**Archivos**: `tools/producer_demo/main.py`

---

### 1.3 Deprecación `event_loop` fixture en conftest de integración

**Estado**: `tests/integration/conftest.py` define un fixture `event_loop`
custom que está deprecado en `pytest-asyncio >= 0.22`. Genera `DeprecationWarning`.

**Qué hacer**: reemplazar por `loop_scope="session"` en los fixtures de
Testcontainers con `pytest_asyncio.fixture(scope="session")` y configurar
`asyncio_mode = "auto"` + `asyncio_default_fixture_loop_scope = "session"`
en `pyproject.toml`.

```toml
# pyproject.toml
[tool.pytest.ini_options]
asyncio_mode = "auto"
asyncio_default_fixture_loop_scope = "session"
```

```python
# tests/integration/conftest.py — eliminar event_loop fixture
# Cambiar pytest_asyncio.fixture a scope="session" en redis_client si aplica
```

**Archivos**: `tests/integration/conftest.py`, `pyproject.toml`

---

## Prioridad 2 — Cobertura de tests faltante

### 2.1 Test de integración end-to-end del ExampleConsumer

**Estado**: `tests/integration/consumers/example/` solo tiene `__init__.py`.
No existe ningún test que verifique el flujo completo: publicar evento →
consumer lo procesa → se persistió en la DB.

**Qué hacer**: crear `tests/integration/consumers/example/test_consumer.py`
que:
- Use los fixtures `kafka_bootstrap`, `postgres_dsn`, `redis_client`, `unique_topic`
- Arranque el DB + consumer
- Publique un evento con `AIOKafkaProducer`
- Espere que se procese (polling + timeout)
- Verifique en Postgres que el registro existe
- Verifique que idempotencia funciona (publicar dos veces el mismo `event_id`)
- Verifique que un evento inválido va al DLQ y no al topic original

**Archivos**: `tests/integration/consumers/example/test_consumer.py` (nuevo)

---

### 2.2 Tests de integración para `Database`

**Estado**: `src/db/database.py` tiene cobertura solo unitaria implícita.
No hay tests contra Postgres real para `execute`, `fetch_one`, `fetch_all`,
`insert_batch`, `call_procedure`.

**Qué hacer**: crear `tests/integration/test_database.py` que:
- Cree una tabla temporal en el fixture
- Pruebe `insert_batch` con N filas y verifique el conteo
- Pruebe `fetch_all` con filtros
- Pruebe que `validate_sql_identifier` bloquea nombres inválidos en `insert_batch`
- Pruebe el retry automático ante un error simulado de deadlock

**Archivos**: `tests/integration/test_database.py` (nuevo)

---

### 2.3 Test unitario para `BaseConsumer._dispatch`

**Estado**: el loop principal del BaseConsumer no tiene tests unitarios. Solo
hay tests de sus componentes individuales (retry, idempotencia, etc.).

**Qué hacer**: crear `tests/unit/core/test_consumer.py` con mocks de
`AIOKafkaConsumer`, `AIOKafkaProducer`, `IdempotencyStore` y `Database` para
verificar que:
- Un mensaje válido llega al handler y se commitea
- Un mensaje duplicado se salta (sin llamar al handler)
- Un `NonRetryableError` va al DLQ sin retry
- Un `RetryableError` se reintenta N veces antes del DLQ
- Un JSON inválido va directamente al DLQ

**Archivos**: `tests/unit/core/test_consumer.py` (nuevo)

---

### 2.4 Umbral mínimo de cobertura

**Estado**: la cobertura se reporta pero no hay `fail_under` configurado.
El CI puede pasar con 10% de cobertura.

**Qué hacer**: en `pyproject.toml`:

```toml
[tool.coverage.report]
fail_under = 80
show_missing = true
exclude_lines = [
    "pragma: no cover",
    "if TYPE_CHECKING:",
    "raise NotImplementedError",
    "if __name__ == .__main__.:",
]
```

**Archivos**: `pyproject.toml`

---

## Prioridad 3 — Implementaciones documentadas pero faltantes

### 3.1 Clase `FairSemaphore` en `src/core/`

**Estado**: el patrón está completamente documentado en
`docs/patterns/concurrency.md` pero la clase no existe en el código.
Está "documentar antes de implementar", que es útil, pero alguien que quiera
usarla tiene que implementarla desde cero.

**Qué hacer**: crear `src/core/concurrency.py`:

```python
import asyncio

class FairSemaphore:
    def __init__(self, value: int) -> None:
        self._sem = asyncio.Semaphore(value)
        self._waiters = 0

    async def acquire(self) -> None:
        self._waiters += 1
        try:
            await self._sem.acquire()
        finally:
            self._waiters -= 1

    def release(self) -> None:
        self._sem.release()

    async def yield_if_others_waiting(self) -> None:
        if self._waiters > 0:
            self.release()
            await asyncio.sleep(0)
            await self.acquire()

    async def __aenter__(self) -> "FairSemaphore":
        await self.acquire()
        return self

    async def __aexit__(self, *_) -> None:
        self.release()
```

**Archivos**: `src/core/concurrency.py` (nuevo), `tests/unit/core/test_concurrency.py` (nuevo)

---

### 3.2 Stubs en `src/services/`

**Estado**: `src/services/` solo tiene `__init__.py`. El plan menciona
servicios opcionales: multi-tenancy, KMS, crypto.

**Qué hacer**: crear stubs documentados que expliquen qué va ahí y cómo
integrarse. No lógica real — solo la firma de la interfaz y el docstring.

```
src/services/
├── __init__.py
├── tenant_service.py   ← multi-tenancy (routing por tenant_id)
├── kms_service.py      ← Key Management Service (encriptación de campos)
└── crypto_service.py   ← firma y verificación de payloads
```

**Archivos**: `src/services/tenant_service.py`, `src/services/kms_service.py`,
`src/services/crypto_service.py` (todos nuevos, stubs)

---

## Prioridad 4 — Experiencia de desarrollo

### 4.1 Pipeline CI/CD

**Estado**: no existe `.github/workflows/`.

**Qué hacer**: crear `.github/workflows/ci.yml` con:

```yaml
name: CI
on: [push, pull_request]
jobs:
  lint:
    steps:
      - uv run ruff check src/ tests/
      - uv run mypy src/
  test-unit:
    steps:
      - uv run pytest tests/unit/ -v --cov=src --cov-report=xml
  test-integration:
    services:
      # docker-in-docker o Testcontainers via DOCKER_HOST
    steps:
      - uv run pytest tests/integration/ -v
  build:
    steps:
      - docker build -t kafka-consumer-template:${{ github.sha }} .
      - Verificar que FastAPI no está en la imagen
```

**Archivos**: `.github/workflows/ci.yml` (nuevo)

---

### 4.2 Pre-commit hooks

**Estado**: no existe `.pre-commit-config.yaml`. Ruff y mypy se ejecutan
manualmente o en CI, pero no al hacer commit.

**Qué hacer**: crear `.pre-commit-config.yaml`:

```yaml
repos:
  - repo: https://github.com/astral-sh/ruff-pre-commit
    rev: v0.8.0
    hooks:
      - id: ruff
        args: [--fix]
      - id: ruff-format
  - repo: local
    hooks:
      - id: mypy
        name: mypy
        entry: uv run mypy src/
        language: system
        pass_filenames: false
```

**Archivos**: `.pre-commit-config.yaml` (nuevo)

---

### 4.3 README del producer demo

**Estado**: `tools/producer_demo/` no tiene `README.md`.

**Qué hacer**: crear `tools/producer_demo/README.md` explicando:
- Qué es y para qué sirve
- Cómo levantarlo
- Los endpoints disponibles
- Ejemplos de payloads con `curl`
- Por qué no va a producción

**Archivos**: `tools/producer_demo/README.md` (nuevo)

---

### 4.4 Guardar `alembic/versions/` en git

**Estado**: la carpeta `alembic/versions/` está vacía y podría no trackearse
en git (dependiendo del `.gitignore`).

**Qué hacer**: agregar un `.gitkeep`:

```bash
touch alembic/versions/.gitkeep
```

**Archivos**: `alembic/versions/.gitkeep` (nuevo)

---

## Prioridad 5 — Hardening de producción

### 5.1 Monitoreo de consumer group lag

**Estado**: las métricas miden mensajes procesados pero no el **lag** del
consumer group (cuántos mensajes están pendientes de procesar en el topic).

**Qué hacer**: agregar un background task que consulte el lag via `aiokafka`
y lo exponga como `Gauge`:

```python
CONSUMER_LAG = Gauge(
    "kafka_consumer_lag",
    "Mensajes pendientes en el topic (lag del consumer group)",
    labelnames=("consumer", "topic", "partition"),
)
```

Actualizar cada 30s desde un background task en `BaseConsumer`.

---

### 5.2 Script de reprocesamiento del DLQ

**Estado**: no hay herramienta para reprocesar mensajes del DLQ.

**Qué hacer**: crear `tools/dlq_reprocessor/main.py` que:
- Lee mensajes del DLQ topic
- Filtra por razón (`x-dlq-reason` header)
- Re-publica en el topic original
- Soporta `--dry-run`, `--limit N`, `--reason filter`

---

### 5.3 Modo multi-consumer en un solo proceso

**Estado**: cada consumer es un proceso independiente. No hay forma de
correr múltiples consumers en un solo pod (útil para consumers ligeros).

**Qué hacer**: crear `src/core/runner.py`:

```python
async def run_all(*consumers: BaseConsumer) -> None:
    """Corre múltiples consumers en paralelo dentro de un mismo event loop."""
    await asyncio.gather(*[c.run() for c in consumers])
```

---

### 5.4 Rate limiting para upstream APIs

**Estado**: documentado en los comentarios de código pero no implementado.
Sin rate limiting, un spike de mensajes puede saturar un upstream.

**Qué hacer**: crear `src/core/rate_limiter.py` con un token bucket sobre
Redis:

```python
class RateLimiter:
    async def acquire(self, key: str, max_per_second: int) -> bool: ...
```

---

### 5.5 Graceful shutdown mejorado

**Estado**: el shutdown espera background tasks con timeout de 30s hardcodeado
en `BaseConsumer._shutdown()`.

**Qué hacer**: hacer el timeout configurable como `shutdown_timeout_seconds`
en `BaseConsumer.__init__()` y agregar un log cuando se agota el timeout con
los nombres de los tasks pendientes.

---

### 5.6 Compresión configurable en producer

**Estado**: el producer usa `compression_type="gzip"` hardcodeado en
`KafkaClientFactory.producer()`. `lz4` o `zstd` son más rápidos.

**Qué hacer**: exponer `compression_type` en `GlobalSettings` y pasarlo
al factory.

---

## Resumen por estado

| # | Item | Prioridad | Archivos afectados |
|---|---|---|---|
| 1.1 | Migración inicial Alembic | 🔴 P1 | `src/db/models.py`, `alembic/env.py`, `alembic/versions/` |
| 1.2 | Deprecación `@app.on_event` | 🔴 P1 | `tools/producer_demo/main.py` |
| 1.3 | Deprecación `event_loop` fixture | 🔴 P1 | `tests/integration/conftest.py`, `pyproject.toml` |
| 2.1 | Test E2E ExampleConsumer | 🟠 P2 | `tests/integration/consumers/example/test_consumer.py` |
| 2.2 | Tests integración Database | 🟠 P2 | `tests/integration/test_database.py` |
| 2.3 | Tests unitarios BaseConsumer | 🟠 P2 | `tests/unit/core/test_consumer.py` |
| 2.4 | Umbral mínimo de cobertura | 🟠 P2 | `pyproject.toml` |
| 3.1 | Clase `FairSemaphore` | 🟡 P3 | `src/core/concurrency.py` |
| 3.2 | Stubs en `src/services/` | 🟡 P3 | `src/services/*.py` |
| 4.1 | Pipeline CI/CD | 🟡 P4 | `.github/workflows/ci.yml` |
| 4.2 | Pre-commit hooks | 🟡 P4 | `.pre-commit-config.yaml` |
| 4.3 | README producer demo | 🟡 P4 | `tools/producer_demo/README.md` |
| 4.4 | `.gitkeep` en `alembic/versions/` | 🟡 P4 | `alembic/versions/.gitkeep` |
| 5.1 | Métrica de consumer group lag | 🔵 P5 | `src/core/metrics.py`, `src/core/consumer.py` |
| 5.2 | Script DLQ reprocessor | 🔵 P5 | `tools/dlq_reprocessor/main.py` |
| 5.3 | Modo multi-consumer | 🔵 P5 | `src/core/runner.py` |
| 5.4 | Rate limiter con Redis | 🔵 P5 | `src/core/rate_limiter.py` |
| 5.5 | Shutdown timeout configurable | 🔵 P5 | `src/core/consumer.py` |
| 5.6 | Compresión configurable en producer | 🔵 P5 | `src/core/client.py`, `src/config/settings.py` |
