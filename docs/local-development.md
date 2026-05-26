# Desarrollo local

## Requisitos

| Herramienta | Versión mínima | Para qué |
|---|---|---|
| Python | 3.13 | Runtime |
| [uv](https://docs.astral.sh/uv/) | 0.5+ | Gestión de dependencias y virtualenv |
| Docker Desktop | Cualquier reciente | Infra local (Redpanda, Redis, Postgres) |

Verificar:

```bash
uv --version
docker --version
docker compose version
```

---

## Setup inicial (una vez)

```bash
# 1. Clonar / posicionarse en el proyecto
cd kafka-consumer-template/

# 2. Instalar todas las dependencias (prod + dev + test + lint)
uv sync

# 3. Copiar y editar variables de entorno
cp .env.example .env
# Editar .env si necesitás cambiar puertos o credenciales

# 4. Levantar la infra local
docker compose up -d

# 5. Verificar que todo esté arriba
docker compose ps
```

La primera vez que corra `docker compose up -d` va a descargar las imágenes
(Redpanda, Redis, Postgres). Tarda 1-2 minutos dependiendo de la conexión.

---

## Infra local — qué levanta docker-compose.yml

| Servicio | Puerto | Acceso |
|---|---|---|
| Redpanda (Kafka) | `9092` | Bootstrap servers para consumers |
| Redpanda Console | `8080` | UI para explorar topics y mensajes |
| Redis | `6379` | Idempotencia y caché |
| PostgreSQL | `5432` | Base de datos del consumer |
| example-consumer | — | Solo con `--profile full` |

Abrir la Redpanda Console en `http://localhost:8080` para inspeccionar topics,
offsets, y mensajes en tiempo real.

---

## Correr el example consumer

```bash
# Terminal 1: consumer
uv run example-consumer

# Logs esperados al arrancar:
# {"timestamp": "...", "level": "info", "event": "consumer_started",
#  "consumer": "example-consumer", "topic": "example-events", ...}
```

El consumer queda corriendo y esperando mensajes. Para pararlo: `Ctrl+C`.

---

## Publicar eventos de prueba (producer demo)

El producer demo es una app FastAPI en `tools/producer_demo/` — solo para
desarrollo local, nunca va a producción.

```bash
# Terminal 2: producer demo
uv run uvicorn tools.producer_demo.main:app --reload --port 8000
```

Abrir `http://localhost:8000/docs` y usar Swagger para publicar eventos:

**POST `/publish/example`**
```json
{
  "type": "greeting",
  "event_id": "evt-001",
  "user_id": "user-123",
  "message": "Hola mundo"
}
```

```json
{
  "type": "farewell",
  "event_id": "evt-002",
  "user_id": "user-123",
  "reason": "Se fue"
}
```

El consumer en Terminal 1 debería mostrar los logs de procesamiento con
`message_id`, `consumer_name`, `topic`, `event_type` automáticamente incluidos
gracias a los ContextVars.

---

## Ver métricas Prometheus

Con el consumer corriendo:

```bash
curl http://localhost:9090/metrics | grep kafka_
```

Métricas disponibles:

```
kafka_messages_total{consumer="example-consumer",topic="example-events",status="success"}
kafka_message_duration_seconds{...}
kafka_consumer_state{consumer="example-consumer"}
kafka_idempotency_duplicates_total{...}
kafka_dlq_total{...}
```

Para probar idempotencia: publicar el mismo evento dos veces con el mismo
`event_id`. La segunda vez el counter `kafka_idempotency_duplicates_total` sube.

---

## Comandos del día a día

```bash
# Instalar dependencias después de cambiar pyproject.toml
uv sync

# Correr tests unitarios (rápidos, sin Docker)
uv run pytest tests/unit/ -v

# Correr tests de integración (necesita Docker)
uv run pytest tests/integration/ -v

# Lint
uv run ruff check src/ tests/
uv run ruff check src/ tests/ --fix    # auto-fix los corregibles

# Type checking
uv run mypy src/

# Migraciones
uv run alembic revision --autogenerate -m "add greetings table"
uv run alembic upgrade head
uv run alembic downgrade -1

# Bajar la infra
docker compose down

# Bajar y borrar volúmenes (reset completo)
docker compose down -v
```

---

## Redpanda Console — tips

Acceder en `http://localhost:8080`:

- **Topics**: ver mensajes raw, offsets por partition, consumer group lag.
- **Consumer Groups**: ver si el group está activo, offset actual vs latest.
- **Schema Registry**: no se usa en el template (usamos Pydantic en código).

Para inspeccionar la DLQ: buscar el topic `example-events-dlq` en la UI.
Los mensajes DLQ tienen headers `x-dlq-reason`, `x-dlq-source-topic`,
`x-dlq-source-partition`, `x-dlq-source-offset`.

---

## Solución de problemas comunes

**El consumer no conecta a Redpanda**
```bash
docker compose ps          # verificar que redpanda esté "healthy"
docker compose logs redpanda | tail -20
```

**Redis connection refused**
```bash
redis-cli -h localhost ping    # debe devolver PONG
docker compose logs redis
```

**`uv run` falla con ModuleNotFoundError**
```bash
uv sync                        # reinstalar dependencias
```

**`alembic upgrade head` falla con connection error**
```bash
# verificar que postgres esté corriendo
docker compose ps postgres
# verificar DATABASE_URL en .env
grep DATABASE_URL .env
```

**Los tests de integración fallan con Docker not running**
```bash
docker info                    # debe mostrar info del daemon
# En WSL: habilitar Docker Desktop WSL integration en settings
```
