# kafka-consumer-template

Template Python para Kafka/Redpanda consumers de producción. Production-ready,
testeable, observable, sin FastAPI en el camino crítico.

## Por qué este template

Los consumers de Kafka son procesos **standalone**, no aplicaciones web.
Este template está diseñado pensando en eso:

- **`src/`** (estándar PyPA), no `app/` (convención FastAPI)
- **Un consumer = un folder** en `src/consumers/<nombre>/` — copiable
- **Handlers como funciones puras** — testeables sin Kafka
- **FastAPI solo como dev tool** en `tools/producer_demo/`, garantizado
  fuera de producción por 4 capas
- **Idempotencia y DLQ** son primitivas del core, no responsabilidad del handler
- **Testing con infra real** (Testcontainers), no mocks de `aiokafka`

## Quick start

```bash
# Requisitos: Python 3.13+, uv, Docker
uv sync
cp .env.example .env
docker compose up -d                    # Redpanda + Redis + Postgres + Console UI

# Levantar el example consumer
uv run example-consumer

# En otra terminal: levantar el dev producer (FastAPI)
uv run uvicorn tools.producer_demo.main:app --reload

# Abrir http://localhost:8000/docs y publicar un evento:
# POST /publish/example
# {
#   "type": "greeting",
#   "user_id": "user-1",
#   "message": "Hola"
# }

# Ver los logs del consumer con message_id, consumer_name, topic propagados via ContextVars.
# Métricas Prometheus en http://localhost:9090/metrics
# Redpanda Console UI en http://localhost:8080
```

## Estructura

```
src/
├── core/           ← Framework (BaseConsumer, exceptions, idempotency, ...)
├── config/         ← Settings globales (Pydantic BaseSettings)
├── db/             ← Wrapper async sobre asyncpg (retry, bulk insert)
├── consumers/      ← Un folder por consumer
│   └── example/    ← Template copiable
└── services/       ← Servicios opcionales (multi-tenancy, KMS, ...)

tools/
└── producer_demo/  ← FastAPI dev tool — NO va a producción

tests/
├── unit/           ← pytest puro, sin infra
└── integration/    ← Testcontainers (Redpanda + Redis + Postgres reales)

docs/
├── creating-a-consumer.md
└── patterns/
    ├── background-tasks.md
    ├── concurrency.md
    └── database.md

alembic/            ← Migraciones DB
k8s/                ← Manifests con exec probe
Dockerfile          ← Multi-stage, --no-dev, sin tools/, layer caching
docker-compose.yml  ← Redpanda + Redis + Postgres + Console UI
CLAUDE.md           ← Guía completa para agentes de IA
```

## Crear un nuevo consumer

```bash
cp -r src/consumers/example/ src/consumers/mi_consumer/
# Editar settings.py (cambiar env_prefix), schemas.py, handlers.py, consumer.py
# Registrar en pyproject.toml: mi-consumer = "src.consumers.mi_consumer.consumer:run"
uv sync
uv run mi-consumer
```

Workflow completo: `docs/creating-a-consumer.md`.

## Comandos

| Comando | Qué hace |
|---|---|
| `uv sync` | Instala todo (deps + dev + test + lint) |
| `uv sync --no-dev` | Sin FastAPI/uvicorn (lo que va a producción) |
| `uv run example-consumer` | Corre el example consumer |
| `uv run pytest tests/unit/` | Tests unitarios (rápidos, sin infra) |
| `uv run pytest tests/integration/` | Tests con Testcontainers |
| `uv run ruff check src/ tests/` | Linting + banned-api (rechaza FastAPI en src/) |
| `uv run mypy src/` | Type checking estricto |
| `uv run alembic revision --autogenerate -m "..."` | Crea migration |
| `uv run alembic upgrade head` | Aplica migrations |
| `docker compose up -d` | Levanta infra local |
| `docker compose --profile full up` | También levanta el consumer |
| `docker build -t kafka-consumer-template .` | Build de producción |

## Capas de defensa anti-FastAPI-en-prod

1. **`pyproject.toml`**: FastAPI en `[dependency-groups] dev`, no en `[project.dependencies]`.
   `uv sync --no-dev` no lo instala.
2. **Wheel**: `[tool.hatch.build.targets.wheel] packages = ["src"]`. `tools/` no se empaqueta.
3. **Dockerfile**: `COPY src/` en el runtime stage. `tools/` no se copia.
4. **Ruff banned-api**: `import fastapi` en `src/` rompe CI antes del commit.

Si las 4 fallan, el contenedor crashea con `ModuleNotFoundError` al arrancar.

## Para agentes de IA

Leer `CLAUDE.md`.
