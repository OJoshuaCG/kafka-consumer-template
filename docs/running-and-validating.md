# Cómo ejecutar y validar el proyecto

Guía paso a paso para verificar que cada capa del proyecto funciona correctamente.
Cubre validación estática, tests, ejecución local, flujo end-to-end y verificación
de la imagen de producción.

---

## Requisitos previos

```bash
# Verificar versiones
uv --version          # >= 0.5
python3 --version     # >= 3.13 (uv lo maneja automáticamente)
docker --version      # cualquier versión reciente con Compose v2
docker compose version
```

---

## Paso 1 — Instalación de dependencias

```bash
uv sync
```

**Salida esperada**: sin errores. El virtualenv `.venv/` se crea en el proyecto.

**Verificar que FastAPI NO está en producción**:

```bash
uv sync --no-dev
.venv/bin/python -c "import fastapi" 2>&1 && echo "FAIL" || echo "OK: fastapi ausente"
# Esperado: OK: fastapi ausente

# Restaurar todas las dependencias para desarrollo
uv sync
```

---

## Paso 2 — Validación estática (sin infra)

### 2a. Ruff (linting + banned-api)

```bash
uv run ruff check src/ tests/
```

**Salida esperada**:
```
All checks passed!
```

Si hay errores, intentar auto-fix:
```bash
uv run ruff check src/ tests/ --fix
```

### 2b. Mypy (type checking estricto)

```bash
uv run mypy src/
```

**Salida esperada**:
```
Success: no issues found in 25 source files
```

### 2c. Compilación de sintaxis

```bash
uv run python -m compileall src/ tools/ tests/ -q
```

**Salida esperada**: sin output (silencioso = OK).

---

## Paso 3 — Tests unitarios

No requieren Docker ni ninguna infra.

```bash
uv run pytest tests/unit/ -v
```

**Salida esperada**:
```
tests/unit/consumers/example/test_handlers.py::TestHandleGreeting::test_persists_valid_event PASSED
tests/unit/consumers/example/test_handlers.py::TestHandleGreeting::test_rejects_empty_message PASSED
tests/unit/consumers/example/test_handlers.py::TestHandleFarewell::test_persists_with_reason PASSED
tests/unit/consumers/example/test_handlers.py::TestHandleFarewell::test_persists_without_reason PASSED
tests/unit/core/test_context.py::... PASSED
tests/unit/core/test_exceptions.py::... PASSED
tests/unit/core/test_retry.py::... PASSED
tests/unit/core/test_utils.py::... PASSED

35 passed in X.XXs
```

Con cobertura:

```bash
uv run pytest tests/unit/ -v --cov=src --cov-report=term-missing
```

---

## Paso 4 — Infra local

```bash
docker compose up -d
```

**Verificar que todo está healthy**:

```bash
docker compose ps
```

**Salida esperada** (todos `Up` o `healthy`):
```
NAME                SERVICE             STATUS              PORTS
redpanda            redpanda            Up (healthy)        0.0.0.0:9092->9092/tcp
redpanda-console    redpanda-console    Up                  0.0.0.0:8080->8080/tcp
postgres            postgres            Up (healthy)        0.0.0.0:5432->5432/tcp
redis               redis               Up (healthy)        0.0.0.0:6379->6379/tcp
```

**Verificar Redpanda**:
```bash
docker exec redpanda rpk cluster info
# Debe mostrar el cluster con 1 broker
```

**Verificar Redis**:
```bash
docker exec -it redis redis-cli ping
# PONG
```

**Verificar Postgres**:
```bash
docker exec -it postgres psql -U kafka -c "\l"
# Lista de bases de datos, debe incluir kafka_consumer
```

---

## Paso 5 — Migraciones de base de datos

> **Nota**: Antes de correr este paso, completar el TODO 1.1 (crear
> `src/db/models.py` y actualizar `alembic/env.py`). Si `alembic/versions/`
> está vacío, este paso no hace nada.

```bash
# Verificar estado de migraciones
uv run alembic current
# Salida esperada: (sin nada, si versions/ está vacío)

# Generar la migración inicial (después de completar TODO 1.1)
uv run alembic revision --autogenerate -m "initial tables"

# Aplicar
uv run alembic upgrade head
# Salida esperada: "Running upgrade  -> xxxx, initial tables"

# Verificar que las tablas existen
docker exec -it postgres psql -U kafka -d kafka_consumer -c "\dt"
# Debe listar: greetings, farewells, alembic_version
```

---

## Paso 6 — Correr el example consumer

```bash
cp .env.example .env     # si no existe todavía

uv run example-consumer
```

**Salida esperada al arrancar**:
```json
{"timestamp": "2024-11-15T10:30:45.123Z", "level": "info", "event": "consumer_started", "environment": "development", "consumer": "example-consumer", "topic": "example-events", "group_id": "example-consumer", "pattern": "sync"}
```

El consumer queda bloqueado esperando mensajes. Dejar corriendo en esta terminal.

**Verificar que el healthcheck se escribe**:
```bash
# En otra terminal:
cat /tmp/healthcheck
# Debe mostrar un timestamp Unix, ej: 1731661845.123456

# Verificar antigüedad
uv run python -c "import os,time; print(f'{time.time() - os.path.getmtime(\"/tmp/healthcheck\"):.1f}s')"
# Debe ser < 15 segundos
```

**Verificar métricas Prometheus**:
```bash
curl -s http://localhost:9090/metrics | grep "kafka_consumer_state"
# kafka_consumer_state{consumer="example-consumer"} 2.0
# (2 = RUNNING)
```

---

## Paso 7 — Publicar eventos (flujo end-to-end)

### 7a. Levantar el producer demo

```bash
# En otra terminal:
uv run uvicorn tools.producer_demo.main:app --reload --port 8000
```

**Salida esperada**:
```
INFO:     Uvicorn running on http://127.0.0.1:8000 (Press CTRL+C to quit)
```

### 7b. Publicar un saludo

```bash
curl -s -X POST http://localhost:8000/publish/example \
  -H "Content-Type: application/json" \
  -d '{"type": "greeting", "user_id": "u-001", "message": "Hola mundo"}' \
  | python3 -m json.tool
```

**Respuesta esperada**:
```json
{
    "status": "published",
    "topic": "example-events",
    "event_id": "550e8400-e29b-41d4-a716-446655440000"
}
```

**En los logs del consumer** (terminal 1) debería aparecer:
```json
{"timestamp": "...", "level": "info", "event": "greeting_processed",
 "consumer_name": "example-consumer", "message_id": "...",
 "user_id": "u-001", "message": "Hola mundo"}
```

### 7c. Publicar una despedida

```bash
curl -s -X POST http://localhost:8000/publish/example \
  -H "Content-Type: application/json" \
  -d '{"type": "farewell", "user_id": "u-001", "reason": "logout"}' \
  | python3 -m json.tool
```

### 7d. Verificar persistencia en la DB

```bash
docker exec -it postgres psql -U kafka -d kafka_consumer \
  -c "SELECT event_id, user_id, message FROM greetings ORDER BY created_at DESC LIMIT 5;"

docker exec -it postgres psql -U kafka -d kafka_consumer \
  -c "SELECT event_id, user_id, reason FROM farewells ORDER BY created_at DESC LIMIT 5;"
```

---

## Paso 8 — Validar idempotencia

```bash
# Publicar el mismo event_id dos veces
EVENT_ID="test-idempotency-$(date +%s)"

curl -s -X POST http://localhost:8000/publish/example \
  -H "Content-Type: application/json" \
  -d "{\"type\": \"greeting\", \"user_id\": \"u-002\", \"message\": \"Test\"}" \
  | python3 -m json.tool
# Guardar el event_id de la respuesta
```

Para probar idempotencia real, publicar manualmente con el mismo `event_id`
usando rpk:

```bash
# Primer envío
docker exec -it redpanda rpk topic produce example-events <<< \
  '{"type":"greeting","event_id":"fixed-id-001","user_id":"u-003","message":"Primera vez"}'

# Segundo envío — mismo event_id
docker exec -it redpanda rpk topic produce example-events <<< \
  '{"type":"greeting","event_id":"fixed-id-001","user_id":"u-003","message":"Segunda vez"}'
```

**Verificar en los logs**: el segundo envío debe mostrar:
```json
{"event": "event_skipped_duplicate", "event_id": "fixed-id-001"}
```

**Verificar la métrica**:
```bash
curl -s http://localhost:9090/metrics | grep idempotency_duplicates
# kafka_idempotency_duplicates_total{...} 1.0
```

**Verificar en Redis** que la key existe:
```bash
docker exec -it redis redis-cli keys "idempotency:example-consumer:*"
# debe listar la key con prefix idempotency:example-consumer:fixed-id-001
```

---

## Paso 9 — Validar DLQ

### Evento con JSON inválido

```bash
docker exec -it redpanda rpk topic produce example-events <<< \
  'esto no es json'
```

**En los logs del consumer**:
```json
{"event": "sent_to_dlq", "reason": "parse_error: json_decode_error: ..."}
```

### Evento con tipo desconocido

```bash
docker exec -it redpanda rpk topic produce example-events <<< \
  '{"type":"unknown_type","event_id":"e-dlq-001","user_id":"u-999"}'
```

**En los logs**: `sent_to_dlq` con `reason: non_retryable`.

### Ver mensajes en el DLQ

```bash
docker exec -it redpanda rpk topic consume example-events-dlq --num 5
```

**O en Redpanda Console**: `http://localhost:8080` → Topics → `example-events-dlq`

---

## Paso 10 — Validar métricas completas

```bash
curl -s http://localhost:9090/metrics | grep "^kafka_"
```

**Salida esperada** (valores varían según los tests realizados):
```
kafka_consumer_state{consumer="example-consumer"} 2.0
kafka_messages_total{consumer="example-consumer",topic="example-events",status="success"} 3.0
kafka_messages_total{consumer="example-consumer",topic="example-events",status="dlq"} 2.0
kafka_messages_total{consumer="example-consumer",topic="example-events",status="duplicate"} 1.0
kafka_idempotency_duplicates_total{consumer="example-consumer",topic="example-events"} 1.0
kafka_dlq_total{consumer="example-consumer",topic="example-events",reason="parse_error"} 1.0
kafka_dlq_total{consumer="example-consumer",topic="example-events",reason="non_retryable"} 1.0
example_greetings_processed_total 2.0
example_farewells_processed_total 1.0
```

---

## Paso 11 — Validar shutdown graceful

```bash
# Con el consumer corriendo, enviar SIGTERM:
# (Ctrl+C en la terminal del consumer, o kill -SIGTERM <pid>)
```

**Salida esperada en los logs**:
```json
{"event": "consumer_stopping", "consumer": "example-consumer"}
{"event": "consumer_stopped", "consumer": "example-consumer"}
```

El consumer debe terminar limpiamente sin stack traces ni errores.

---

## Paso 12 — Tests de integración (requiere Docker)

```bash
uv run pytest tests/integration/ -v
```

**Salida esperada**:
```
tests/integration/test_idempotency.py::TestIdempotencyStore::test_first_claim_succeeds PASSED
tests/integration/test_idempotency.py::TestIdempotencyStore::test_second_claim_fails PASSED
tests/integration/test_idempotency.py::TestIdempotencyStore::test_namespace_isolation PASSED
tests/integration/test_idempotency.py::TestIdempotencyStore::test_release_allows_reclaim PASSED
tests/integration/test_idempotency.py::TestIdempotencyStore::test_has_been_processed PASSED

5 passed in X.Xs
```

Los contenedores se levantan solos via Testcontainers. La primera vez tarda
más porque descarga las imágenes de Docker.

---

## Paso 13 — Build y validación de imagen Docker

```bash
# Build
docker build -t kafka-consumer-template:test .
```

**Salida esperada**: sin errores. Dos stages (builder + runtime).

### Verificar que FastAPI NO está en la imagen

```bash
docker run --rm kafka-consumer-template:test \
  python -c "import fastapi" 2>&1 \
  && echo "FAIL: fastapi encontrado en imagen" \
  || echo "OK: fastapi ausente de la imagen de produccion"
# Esperado: OK: fastapi ausente de la imagen de produccion
```

### Verificar que `tools/` NO está en la imagen

```bash
docker run --rm kafka-consumer-template:test ls /app/tools 2>&1 \
  | grep -q "No such" \
  && echo "OK: tools/ ausente" \
  || echo "FAIL: tools/ encontrado en imagen"
# Esperado: OK: tools/ ausente
```

### Verificar que el entry point existe

```bash
docker run --rm kafka-consumer-template:test \
  python -c "from src.consumers.example.consumer import run; print('OK: entry point importable')"
# Esperado: OK: entry point importable
```

### Verificar que corre con la infra local

```bash
docker run --rm \
  --network kafka-consumer-template_default \
  -e KAFKA_BOOTSTRAP_SERVERS=redpanda:9092 \
  -e DATABASE_URL=postgresql://kafka:kafka@postgres:5432/kafka_consumer \
  -e REDIS_URL=redis://redis:6379/0 \
  -e ENVIRONMENT=production \
  kafka-consumer-template:test \
  example-consumer &

sleep 5

# Verificar que está corriendo y escribiendo el healthcheck
docker inspect $(docker ps -lq) --format '{{.State.Status}}'
# running

# Detener
docker stop $(docker ps -lq)
```

---

## Resumen de validaciones

| Paso | Validación | Sin infra | Requiere Docker |
|---|---|---|---|
| 1 | Dependencias instaladas + FastAPI excluida | ✓ | |
| 2 | Ruff + mypy + sintaxis | ✓ | |
| 3 | 35 tests unitarios pasan | ✓ | |
| 4 | Infra local healthy | | ✓ |
| 5 | Migraciones aplicadas | | ✓ |
| 6 | Consumer arranca y escribe healthcheck | | ✓ |
| 7 | Evento publicado → procesado → persistido | | ✓ |
| 8 | Segundo envío del mismo event_id → skip | | ✓ |
| 9 | JSON inválido → DLQ | | ✓ |
| 10 | Métricas Prometheus correctas | | ✓ |
| 11 | Shutdown limpio | | ✓ |
| 12 | Tests de integración con Testcontainers | | ✓ |
| 13 | Imagen Docker sin FastAPI ni tools/ | | ✓ |

Los pasos 1-3 se pueden ejecutar en cualquier máquina con `uv`. Los pasos 4-13
requieren Docker Desktop con integración WSL habilitada (en Windows/WSL).

---

## Comandos de limpieza

```bash
# Bajar la infra local
docker compose down

# Bajar y borrar volúmenes (reset completo de datos)
docker compose down -v

# Borrar la imagen de test
docker rmi kafka-consumer-template:test

# Limpiar Redis (idempotencia + caché)
docker exec -it redis redis-cli flushall

# Limpiar base de datos
docker exec -it postgres psql -U kafka -d kafka_consumer \
  -c "TRUNCATE greetings, farewells RESTART IDENTITY;"

# Borrar el healthcheck file
rm -f /tmp/healthcheck

# Limpiar caché de mypy y ruff
rm -rf .mypy_cache .ruff_cache .pytest_cache
```
