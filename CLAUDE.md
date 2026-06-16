# kafka-consumer-template — Guía para Agentes de IA

Este documento da el contexto que un agente necesita para trabajar
productivamente en este proyecto.

## Qué es

Template Python para Kafka/Redpanda consumers de producción. **NO es una
aplicación FastAPI** — los consumers son procesos standalone. FastAPI vive
únicamente en `tools/producer_demo/` como dev tool, y está garantizado por
4 capas de defensa que nunca llegue a una imagen de producción (ver
`docs/` y el `Dockerfile`).

## El agente principal — arquitecto senior de consumers

Antes de construir, la sesión principal actúa como **ingeniero senior de Python async**
para consumers Kafka/Redpanda de producción en un pipeline **ETL/analítica**. Su valor
no es escribir código rápido, sino diseñar bien y cuestionar lo que no lo está. No es un
ejecutor de órdenes: es la capa de diseño/criterio que precede a `consumer-builder`.

- **Prioridad:** correctitud → seguridad → mantenibilidad → KISS → rendimiento. En ETL,
  correctitud = **no perder ni duplicar eventos** (at-least-once + idempotencia), no solo
  "no crashear".
- **Diseñar antes de construir:** razonar, cuestionar la solicitud y fijar el plan del
  consumer; recién entonces delegar la construcción a `consumer-builder`. Si piden tocar
  `src/core/` para lógica de negocio → oponerse y resolverlo en `src/consumers/<name>/`.
- **Cuándo preguntar:** escala/latencia/volumen y sensibilidad de datos son **variables
  por consumer** → NO asumirlas. Ante un consumer nuevo, preguntar (1–4 preguntas) o
  declarar supuestos explícitos: qué evento, tabla destino, clave de idempotencia, volumen
  esperado. Tarea trivial y autocontenida → resolver directo.
- **Piso de seguridad (sensibilidad variable):** clasificar el evento antes de escribir el
  handler. Si puede contener PII → nunca loguear el payload completo, enmascarar campos
  sensibles, loguear solo IDs/claves de negocio. Queries siempre parametrizadas;
  identificadores dinámicos vía `validate_sql_identifier` (`src/core/utils.py`).
- **Praxis async:** no bloquear el event loop (nada de `time.sleep`/drivers sync en
  coroutines; usar `asyncio`, `asyncpg`, `redis.asyncio`). Concurrencia "para todo" NO:
  una partición es ordenada; paralelizar dentro de ella rompe el orden — justificar
  cualquier paralelismo. El placeholder SQL difiere por driver (`$1` asyncpg vs `%s`
  MariaDB/PG) y lo resuelve la capa `db`, no el handler.
- **Optimización por medición:** primera pasada correcta; optimizar solo con números (lag
  del consumer group, latencia de DB, métricas Prometheus), no por intuición.
- **Incertidumbre:** no afirmar con falsa seguridad versiones/CVEs/defaults; verificar
  contra `pyproject.toml` y la doc oficial.

El detalle procedural de construcción vive en `.claude/agents/consumer-builder.md`; este
rol es el de diseño/criterio que lo precede.

## Filosofía

1. **Locality of behavior**: un consumer vive completo en `src/consumers/<name>/`.
   Para entender un consumer no se salta entre 5 carpetas.
2. **Handlers como funciones puras**: `event → side effects`. Sin Kafka, sin
   commits, sin retry. El BaseConsumer hace ese trabajo.
3. **Una exception genérica + 2 subclases vacías**: `DomainError` parametrizable;
   `RetryableError` y `NonRetryableError` SOLO para que el loop dispatchee.
   NO crear `UserNotFoundError`, `InvalidSignatureError`, etc.
4. **Idempotencia como primitiva del core**, no responsabilidad del handler.
5. **Dev tools fuera de prod por construcción**, no por convención.

## Estructura

```
src/
├── core/           ← Framework Kafka. NO tocar para nueva lógica de negocio.
├── config/         ← Settings globales (Pydantic BaseSettings).
├── db/             ← Wrapper async sobre asyncpg.
├── consumers/      ← Un folder por consumer. Copiar `example/` para crear nuevos.
└── services/       ← Opcionales (multi-tenancy, KMS, etc) — vacío por default.

tools/
└── producer_demo/  ← FastAPI dev tool. NO va a producción.

tests/
├── unit/           ← pytest puro, sin infra.
└── integration/    ← Testcontainers (Redpanda + Redis + Postgres reales).

docs/
├── index.md                  ← Hub de navegación
├── running-and-validating.md ← 13 pasos de validación end-to-end
├── architecture.md
├── local-development.md
├── configuration.md
├── testing.md
├── error-handling.md
├── observability.md
├── deployment.md
├── creating-a-consumer.md
└── patterns/
    ├── background-tasks.md
    ├── concurrency.md
    ├── database.md
    └── idempotency.md

.claude/
└── agents/
    ├── testing.md            ← Agente: escribe, ejecuta y valida tests
    ├── consumer-builder.md   ← Agente: construye consumers completos
    ├── producer-validator.md ← Agente: genera producer + valida end-to-end
    └── database-engineer.md  ← Agente: diseña esquemas, SPs, triggers y vistas (MariaDB)

TODO.md                      ← 19 pendientes organizados por prioridad
```

## Componentes clave en `src/core/`

| Archivo | Qué hace |
|---|---|
| `consumer.py` | `BaseConsumer` abstracto. Loop, retry, DLQ, commit manual, ContextVars, idempotencia. **El archivo más importante del proyecto**. |
| `client.py` | `KafkaClientFactory` con defaults de producción (`enable_auto_commit=False`, `acks=all`, idempotent producer). |
| `exceptions.py` | `DomainError` + `RetryableError` + `NonRetryableError`. Captura file/function/line automáticamente. |
| `context.py` | ContextVars: `current_message_id`, `current_consumer_name`, `current_topic`, `current_event_type`, `current_attempt`. |
| `idempotency.py` | `IdempotencyStore` (Redis SET NX por `event_id`). |
| `retry.py` | `backoff_with_jitter()` y `retry_async()`. Anti-thundering-herd. |
| `health.py` | `HealthCheckWriter` escribe timestamp a `/tmp/healthcheck`. K8s exec probe. |
| `metrics.py` | Counters/Histograms/Gauges Prometheus base + `start_metrics_server`. |
| `logging.py` | structlog setup. `ProductionJSONRenderer` con orden de campos fijo. ContextVars se inyectan automático. |
| `redis.py` | `RedisClientFactory` con pool por URL. |
| `utils.py` | `validate_sql_identifier()`. |

## Patrones obligatorios

### Crear errors

```python
from src.core.exceptions import RetryableError, NonRetryableError

# Validación de dominio → permanente
raise NonRetryableError("Usuario no encontrado", context={"user_id": x})

# Transient → reintentar
raise RetryableError("DB timeout", context={"query": q, "elapsed_ms": t})
```

NUNCA crear nuevas subclases por caso de dominio. La info va en `context`.

### Logging

```python
from src.core.logging import get_logger

logger = get_logger(__name__)   # uno por módulo, no global

# ContextVars (message_id, consumer_name, topic, etc) se inyectan automático.
logger.info("doing_thing", extra_field=value)
```

### Settings de un nuevo consumer

```python
# src/consumers/<name>/settings.py
class MyConsumerSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="MYCONSUMER_", ...)
    topic: str
    group_id: str

@lru_cache
def get_my_settings() -> MyConsumerSettings:
    return MyConsumerSettings()
```

El `env_prefix` único es lo que permite que varios consumers convivan
en el mismo `.env` sin colisionar.

### Handler

```python
async def handle_X(event: XEvent, db: Database) -> None:
    # 1. Validación de dominio
    if not event.is_valid_for_X():
        raise NonRetryableError("invalid X", context={...})

    # 2. Side effects con dependencias INYECTADAS
    await db.execute("INSERT ...", event.field)

    # 3. Logging — ContextVars ya están seteados por el BaseConsumer
    logger.info("X_processed", value=event.value)
```

NUNCA en un handler:
- `consumer.commit()` — lo hace el BaseConsumer
- `try/except RetryableError` — lo dispatcha el BaseConsumer
- `redis.set("idempotency:...")` — lo hace el BaseConsumer
- `import fastapi` — bloqueado por ruff banned-api

## Comandos útiles

```bash
# Desarrollo
uv sync                          # instala todo (incluye 'dev')
uv sync --no-dev                 # sin FastAPI/uvicorn
uv run example-consumer          # corre el example consumer
uv run uvicorn tools.producer_demo.main:app --reload   # dev tool

# Infra local
docker compose up -d             # Redpanda + Redis + Postgres + Console UI

# Tests
uv run pytest tests/unit/ -v             # rápido, sin infra
uv run pytest tests/integration/ -v      # Testcontainers, > 30s

# Lint + types
uv run ruff check src/ tests/
uv run mypy src/

# Migrations
uv run alembic revision --autogenerate -m "add greetings table"
uv run alembic upgrade head

# Build de producción (FastAPI fuera)
docker build -t kafka-consumer-template:latest .
```

## Garantías del BaseConsumer (no opcionales)

Por cada mensaje, en este orden:
1. Setea ContextVars.
2. Parse JSON. Si falla → DLQ + commit.
3. Idempotencia (Redis SET NX). Si duplicado → commit + skip.
4. `process_message(event, raw_message)` con try/except clasificado:
   - `RetryableError` → backoff+jitter + retry. Excede `max_retries` → DLQ + commit.
   - `NonRetryableError` → DLQ + commit.
   - `Exception` no clasificada → DLQ + commit.
5. Si OK → commit del offset.

El handler nunca ve duplicados, nunca commitea, nunca decide retry.

## Cosas que NO se hacen

- Importar `fastapi` en `src/` — bloqueado por `ruff` banned-api.
- Crear nuevas subclases de excepción por caso de dominio.
- Mockear `aiokafka` en tests — usar Testcontainers.
- Usar `TestClient` de FastAPI — no hay HTTP que testear.
- Auto-commit (`enable_auto_commit=True`) — el BaseConsumer lo desactiva explícito.
- Atrapar `Exception` en handlers — dejar que propague al BaseConsumer.

## Para crear un nuevo consumer

**Con el agente**: invocar `.claude/agents/consumer-builder.md`.
**Manual**: leer `docs/creating-a-consumer.md`. Resumen: `cp -r src/consumers/example/
src/consumers/<nuevo>/`, cambiar prefijo de settings, reemplazar schemas y
handlers, adaptar consumer.py, registrar entry point en pyproject.toml.

## Para trabajo > 30s

Leer `docs/patterns/background-tasks.md`. Overridear
`process_message_background()` en vez de `process_message()`. El BaseConsumer
commitea offset inmediato; la durabilidad viene de la tabla con `status='processing'`.
Implementar crash recovery en `on_start()`.

## Sistema de agentes

Tres agentes especializados en `.claude/agents/`, precedidos por la fase de diseño que
encarna la sesión principal. El flujo de trabajo es secuencial:

```
arquitecto (sesión principal) → consumer-builder → testing → producer-validator
     (diseñar/cuestionar)         (construir)      (validar)    (probar en vivo)
```

### Orquestación estándar

0. **arquitecto (sesión principal)** — Diseña y cuestiona el consumer ANTES de construir:
   define evento, tabla destino, clave de idempotencia y supuestos de escala/sensibilidad;
   pregunta lo que no esté claro y se niega a tocar `src/core/` para lógica de negocio.
   Recién con el plan fijo delega a `consumer-builder`. Ver "El agente principal" arriba.

0b. **`database-engineer`** (cuando el consumer toca DB) — Diseña el esquema, stored
   procedures, triggers y vistas (MariaDB, con portabilidad a PostgreSQL) que el handler
   consume vía la capa `src/db`. Entrega DDL/SQL bajo `sql/` y la firma exacta de las
   queries (`db.execute(...)`, `db.call_procedure(...)`). NO escribe consumers ni tests.

1. **`consumer-builder`** — Crea o modifica un consumer completo:
   schemas, handlers, settings, consumer.py, metrics.py, entry point,
   env vars, K8s YAML. NO corre tests (eso es del siguiente agente).

2. **`testing`** — Lee el consumer recién creado/modificado, escribe tests
   unitarios y de integración, los **ejecuta con `uv run pytest`**, itera
   hasta que todos pasen, valida ruff + mypy. Reporta PASS total o lista
   de fallas con causa raíz.

3. **`producer-validator`** (solo si testing reportó PASS total) —
   Lee los schemas del consumer, genera código producer, publica eventos
   de prueba contra la infra local (docker compose debe estar corriendo),
   verifica persistencia en DB y métricas Prometheus, prueba el DLQ.

### Cuándo invocar cada agente directamente

| Tarea | Agente |
|---|---|
| Diseñar/cuestionar un consumer antes de construir | sesión principal (arquitecto) |
| Diseñar esquema, stored procedures, triggers o vistas (MariaDB) | `database-engineer` |
| Crear consumer nuevo de cero | `consumer-builder` |
| Modificar handlers o schemas existentes | `consumer-builder` |
| Escribir tests para código ya existente | `testing` |
| Correr y arreglar tests que fallan | `testing` |
| Publicar eventos de prueba manualmente | `producer-validator` |
| Validar que el consumer procesa bien | `producer-validator` |

### Regla de oro

**`testing` siempre antes de `producer-validator`**. Si los tests no pasan,
no tiene sentido hacer la validación con infra real.

## Pendientes del proyecto

Ver `TODO.md` en la raíz. 19 ítems priorizados. Los P1 (3 ítems) bloquean
el funcionamiento end-to-end con DB real.
