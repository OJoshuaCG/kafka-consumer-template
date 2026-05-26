# Arquitectura

## Qué es este proyecto

Template para construir **Kafka/Redpanda consumers** en Python — procesos standalone,
no aplicaciones web. La diferencia es importante: un consumer vive en un loop
infinito leyendo mensajes, no sirviendo requests HTTP.

El objetivo del template es que el developer que crea un nuevo consumer solo
necesite implementar:
1. Sus schemas Pydantic.
2. Sus funciones handler (event → side effects).
3. Sus settings con el prefijo de env vars.

Todo lo demás — idempotencia, retry, DLQ, ContextVars, commit manual, métricas,
health check — lo hace el `BaseConsumer`.

---

## Estructura de carpetas

```
src/
├── core/           ← Framework. No tocar para lógica de negocio.
│   ├── consumer.py         ← BaseConsumer (el archivo más importante)
│   ├── client.py           ← KafkaClientFactory (defaults de producción)
│   ├── exceptions.py       ← DomainError + 2 subclases vacías
│   ├── context.py          ← ContextVars (tracing automático)
│   ├── idempotency.py      ← IdempotencyStore (Redis SET NX)
│   ├── retry.py            ← backoff_with_jitter(), retry_async()
│   ├── health.py           ← HealthCheckWriter (/tmp/healthcheck)
│   ├── metrics.py          ← Counters/Histograms/Gauges base
│   ├── logging.py          ← structlog + ProductionJSONRenderer
│   ├── redis.py            ← RedisClientFactory
│   └── utils.py            ← validate_sql_identifier()
├── config/
│   └── settings.py         ← GlobalSettings (env vars compartidas)
├── db/
│   └── database.py         ← Wrapper asyncpg con retry + bulk insert
├── consumers/
│   └── example/            ← Template copiable para un consumer real
│       ├── consumer.py     ← Subclase de BaseConsumer
│       ├── handlers.py     ← Funciones puras: event → side effects
│       ├── schemas.py      ← Pydantic models con discriminated union
│       ├── settings.py     ← BaseSettings con env_prefix único
│       └── metrics.py      ← Métricas específicas del consumer
└── services/               ← Opcionales (multi-tenancy, KMS, etc.)
```

El principio es **locality of behavior**: para entender el consumer de WhatsApp,
leer `src/consumers/whatsapp/`. No hay que saltar entre 5 carpetas.

---

## BaseConsumer — garantías del loop

Por cada mensaje recibido, en este orden exacto:

```
┌─ Mensaje recibido ────────────────────────────────────────────────┐
│                                                                    │
│  1. Setear ContextVars                                            │
│     message_id, consumer_name, topic, event_type, attempt        │
│                                                                    │
│  2. Parse JSON                                                    │
│     ✗ Falla → DLQ + commit                                       │
│                                                                    │
│  3. Idempotencia (Redis SET NX por event_id)                      │
│     ✗ Duplicado → commit + skip (sin invocar handler)            │
│                                                                    │
│  4. process_message(event, raw_message)                           │
│     ✗ RetryableError    → backoff + jitter + retry               │
│                           (si supera max_retries → DLQ + commit)  │
│     ✗ NonRetryableError → DLQ + commit (inmediato)               │
│     ✗ Exception genérica → DLQ + commit (+ log con stack)        │
│     ✓ OK               → commit del offset                       │
│                                                                    │
│  5. Métricas actualizadas                                         │
└───────────────────────────────────────────────────────────────────┘
```

El handler **nunca** ve duplicados, **nunca** decide retry, **nunca** commitea.

### Modo background (opt-in)

Si `process_message_background()` está overrideado, el flujo cambia en el paso 4:

1. El mensaje se persiste con `status='processing'` en la DB.
2. El offset se commitea **inmediatamente**.
3. El trabajo real corre como `asyncio.Task` en background.
4. Al terminar: `status='done'` o `status='failed'`.
5. Al arrancar: `on_start()` retoma los `status='processing'` (crash recovery).

Ver [patterns/background-tasks.md](patterns/background-tasks.md).

---

## Decisiones de diseño

### Por qué no ABC

`BaseConsumer` no hereda de `ABC`. El contrato es: override `process_message()`
**OR** `process_message_background()`, pero no ambos. `@abstractmethod` no puede
expresar un OR exclusivo — forzaría a definir ambos o ninguno.

La convención es que ambos lanzan `NotImplementedError` por defecto. Si una
subclase no override ninguno, el error aparece en tiempo de ejecución, no de
clase (trade-off aceptable dado que hay tests).

### Por qué una sola clase de excepción

`DomainError` + `RetryableError` + `NonRetryableError` es todo lo que existe.
No hay `UserNotFoundError`, `InvalidSignatureError`, `DBTimeoutError`, etc.

Razones:
- La semántica importante para el loop es **retryable vs no retryable**. Todo
  lo demás es detalles que van en el `context` del error.
- Cientos de clases de error hacen que el `except` del BaseConsumer sea
  imposible de mantener y que los tests dependan de jerarquías frágiles.
- `DomainError` captura automáticamente file/function/line/code del caller
  vía `inspect.stack()[2]`, por lo que el log siempre tiene el origen exacto.

### Por qué ContextVars y no parámetros

Pasar `message_id`, `consumer_name`, etc. como parámetros a cada función
significaría contaminar la firma de todos los handlers y funciones de servicio.
Con ContextVars, cualquier `logger.info()` en cualquier nivel del stack incluye
automáticamente esos campos sin que el código lo sepa.

### Por qué commit manual

`enable_auto_commit=False` (hardcodeado en `KafkaClientFactory`). El commit
ocurre **después** de que el handler retorna sin error. Esto garantiza
**at-least-once delivery**: si el consumer muere entre recibir y procesar,
el mensaje se reentrega al restart.

La idempotencia convierte at-least-once en **effectively-exactly-once** en
la capa de aplicación.

### Por qué FastAPI solo en dev tools

FastAPI es una dependencia que trae un grafo enorme (starlette, pydantic extras,
anyio, etc.). Un consumer no necesita HTTP para funcionar. Tener FastAPI en
producción es riesgo de superficie de ataque gratuita.

Las 4 capas de defensa (`dependency-groups`, wheel, Dockerfile, ruff banned-api)
garantizan que nunca llegue a una imagen de producción.

---

## Flujo de datos completo

```
Redpanda Topic
     │
     ▼
AIOKafkaConsumer.getmany()
     │  (batch de mensajes)
     ▼
BaseConsumer._dispatch()   ← por cada mensaje
     │
     ├─► ContextVars.set()
     │
     ├─► orjson.loads(raw_message.value)
     │
     ├─► IdempotencyStore.claim(event_id)
     │         └─ Redis SET NX EX 7d
     │
     ├─► process_message(event, raw_message)
     │         └─ Pydantic validate_python()
     │         └─ handler(parsed_event, db)
     │               └─ DB / Redis / API calls
     │
     ├─► AIOKafkaConsumer.commit()   ← solo si OK
     │
     └─► Prometheus metrics
```

---

## Dependencias clave

| Librería | Rol |
|---|---|
| `aiokafka` | Cliente Kafka async (consumer + producer) |
| `pydantic` v2 | Validación y serialización de eventos |
| `pydantic-settings` | Settings desde env vars |
| `structlog` | Logging estructurado (JSON en prod, console en dev) |
| `orjson` | Deserialización JSON rápida |
| `asyncpg` | Driver PostgreSQL async |
| `redis[hiredis]` | Cliente Redis async con backend C para performance |
| `prometheus-client` | Exposición de métricas `/metrics` |
| `alembic` | Migraciones de base de datos |
