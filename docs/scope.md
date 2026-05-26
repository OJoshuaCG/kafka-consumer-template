# Alcance del proyecto

## Qué es

Template Python para construir **Kafka/Redpanda consumers** de producción. Un consumer
es un **proceso standalone** (no un servicio HTTP) que corre en un loop:

1. Lee mensajes de un topic Kafka.
2. Los valida con Pydantic.
3. Ejecuta lógica de negocio (el handler).
4. Commitea el offset solo si el procesamiento fue exitoso.

El template provee toda la "plomería" del loop para que el developer solo tenga que
escribir la lógica de negocio.

---

## Qué cubre el template

### Garantías del loop (automáticas — el developer no las escribe)

| Garantía | Cómo | Dónde |
|---|---|---|
| At-least-once delivery | Commit manual post-handler | `BaseConsumer` |
| Effectively-exactly-once | Idempotencia Redis SET NX por `event_id` | `IdempotencyStore` |
| Retry con backoff | Exponential jitter, anti-thundering-herd | `retry.py` |
| DLQ | Mensajes irrecuperables → topic separado | `BaseConsumer` |
| Trazabilidad automática | ContextVars en todos los logs | `context.py` |
| Graceful shutdown | Espera que el mensaje en proceso termine | `BaseConsumer` |
| Health check | Timestamp a `/tmp/healthcheck` para K8s exec probe | `health.py` |

### Observabilidad incluida

- **Logs JSON estructurados** con campos fijos y orden garantizado
  (`timestamp → level → event → environment → …`). Compatible con Datadog, Loki, CloudWatch.
- **7 métricas Prometheus base**: mensajes procesados, errores, DLQ, en proceso,
  latencia (histograma), duplicados, reintentos. Endpoint `/metrics` en puerto configurable.

### Herramientas de desarrollo

- `docker-compose.yml` con Redpanda + Redis + Postgres + Redpanda Console UI.
- `tools/producer_demo/` — FastAPI local para publicar eventos de prueba (no va a producción).
- Tests unitarios sin infra (handlers como funciones puras).
- Tests de integración con Testcontainers (infra real efímera en Docker).

### Patterns opcionales implementados

| Pattern | Cuándo usarlo | Referencia |
|---|---|---|
| Background tasks | Trabajo > 30s | `docs/patterns/background-tasks.md` |
| Bulk insert | INSERT de muchas filas a la vez | `docs/patterns/database.md` |
| Fair semaphore | Limitar concurrencia en background tasks | `docs/patterns/concurrency.md` |
| SQL identifier validation | Nombres de tabla dinámicos | `core/utils.py` |

---

## Qué implementa el developer

Solo **tres archivos** por consumer nuevo:

```
src/consumers/mi-consumer/
├── schemas.py     ← Pydantic models para los eventos del topic
├── handlers.py    ← Funciones puras: async def handle_X(event, db) -> None
└── settings.py    ← Env vars con env_prefix único (ej: "MICONS_")
```

El resto (`consumer.py`, `metrics.py`) se copia del example y se ajusta en
5-10 líneas. Ver [Crear un consumer](creating-a-consumer.md).

---

## Qué NO cubre

### No es un framework web

No hay HTTP, no hay routers, no hay middleware HTTP. Si necesitás una API REST,
usá FastAPI directamente (este template no es el lugar).

### No gestiona topics ni schemas Kafka

No crea topics, no configura particiones ni replicación, no interactúa con
Schema Registry. Asume que el topic ya existe y está configurado por tu equipo
de plataforma o por el productor.

### No orquesta múltiples consumers en un solo proceso

Cada imagen Docker corre **un consumer**. Si tenés 5 consumers, tenés 5 Deployments.
No hay runner multi-consumer embebido (es implementable con `asyncio.gather` —
ver `TODO.md`).

### No implementa Circuit Breaker

El retry con jitter protege contra thundering herd pero no implementa Circuit Breaker
para dependencias externas (DB caída, API lenta). Si lo necesitás, podés agregar
`tenacity` con `stop_after_attempt + wait_exponential` en el handler.

### No incluye Distributed Tracing (OpenTelemetry)

Los ContextVars dan trazabilidad dentro del proceso (todos los logs de un mensaje
tienen el mismo `message_id`), pero no propagan spans a Jaeger/Zipkin/Tempo.
La integración con OTel es un TODO explícito (ver `TODO.md`).

### No integra Schema Registry

Los eventos se serializan/deserializan como JSON. No hay integración con
Confluent Schema Registry ni soporte Avro/Protobuf nativo.

### No implementa Rate Limiter

No hay control de TPS en el consumer. Si el downstream tiene límites de rate,
el handler debe implementarlo (o agregar `asyncio.sleep` / token bucket).

---

## Dependencias externas requeridas

| Servicio | Necesario | Condición |
|---|---|---|
| **Kafka / Redpanda** | ✅ Siempre | Es el bus — sin esto no hay consumer |
| **Redis** | ✅ Por defecto | Idempotencia. Opcional si se desactiva (ver [deploy-existing-infra.md](deploy-existing-infra.md#sin-redis)) |
| **PostgreSQL** | Solo si el consumer persiste datos | Si el handler no usa `Database`, no se conecta |
| Redpanda Console | Solo dev local | UI para inspeccionar topics y mensajes |

**Redis es la única dependencia extra respecto a Kafka.** La idempotencia puede
desactivarse si tu handler ya es naturalmente idempotente o si tu entorno no
tiene Redis disponible.

---

## En qué se diferencia de otras alternativas

| | Este template | Faust / Flink | Kafka Streams |
|---|---|---|---|
| Lenguaje | Python | Python | Java/Scala |
| Complejidad | Baja | Media-Alta | Alta |
| Stateful processing | No nativo | Sí | Sí |
| Schema Registry | No | Opcional | Opcional |
| Caso de uso ideal | Consumers de dominio simples/medios | Pipelines de streaming complejos | Joins, agregaciones stateful |
| Overhead de aprendizaje | Bajo (asyncio + aiokafka) | Medio | Alto |

Este template es la opción correcta cuando tenés un consumer que:
- Recibe un evento → hace algo (DB, API, otro topic) → commitea.
- No necesita joins entre topics ni estado distribuido.
- Quiere observabilidad y resiliencia sin escribir el loop a mano.
