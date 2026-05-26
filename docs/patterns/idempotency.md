# Pattern: Idempotencia con Redis SET NX

## Por qué es necesaria

Kafka garantiza **at-least-once delivery**: si el consumer se cae después de
procesar un mensaje pero antes de commitear el offset, ese mensaje se
reentrega al reiniciar. Sin idempotencia, el handler se ejecutaría dos veces
para el mismo evento.

Casos que producen re-entregas:
- Pod reiniciado por K8s (OOMKilled, liveness probe, deploy)
- Rebalance de particiones (otro consumer toma la misma partición)
- Reset manual de offsets (reprocesamiento intencional)
- Fallos en el commit (red cortada justo después de procesar)

La idempotencia convierte **at-least-once** en **effectively-exactly-once**
en la capa de aplicación.

---

## Cómo funciona

`IdempotencyStore` usa **Redis SET NX** (Set if Not eXists) con TTL:

```
SETEX idempotency:{namespace}:{event_id} {ttl_seconds} "1" NX
```

- `NX`: solo setea si la key NO existe → operación atómica
- `EX {ttl}`: expira automáticamente (default: 7 días)
- `{namespace}`: nombre del consumer, evita colisiones entre consumers

El BaseConsumer llama `IdempotencyStore.claim(event_id)` antes de invocar
el handler:

- Retorna `True` → evento nuevo, procesar normalmente
- Retorna `False` → evento duplicado, commitear y saltar sin invocar el handler

```
Mensaje recibido
       │
       ▼
claim(event_id)
       │
  ┌────┴────┐
  │         │
True       False
  │         │
  ▼         ▼
Handler   commit()
corre     + skip
  │
  ▼
commit()
```

---

## event_id — qué se usa como clave

El BaseConsumer extrae el `event_id` en este orden de prioridad:

1. `event["event_id"]` — campo explícito preferido
2. `event["id"]` — alias alternativo
3. `"{topic}-{partition}-{offset}"` — sintético basado en la posición Kafka

La tercera opción garantiza idempotencia incluso si el evento no tiene un ID
semántico, pero es menos robusta ante re-publicaciones (mismo contenido,
diferente offset → no detecta duplicado).

**Recomendación**: incluir siempre `event_id` en el payload del evento. Usar
UUIDs v4 o similar.

```json
{
  "type": "order_received",
  "event_id": "550e8400-e29b-41d4-a716-446655440000",
  "order_id": "ord-001",
  "total": 99.99
}
```

---

## Configuración

```python
# En BaseConsumer.__init__:
self._idempotency = IdempotencyStore(
    redis,
    namespace=idempotency_namespace or self.name,  # default: nombre del consumer
    ttl_seconds=idempotency_ttl_seconds,           # default: 7 días
)
```

Para cambiar el TTL:

```python
consumer = MiConsumer(
    ...
    idempotency_ttl_seconds=3 * 24 * 3600,  # 3 días
)
```

---

## Uso directo (fuera del BaseConsumer)

Si necesitás usar idempotencia en otro contexto (job batch, API, etc.):

```python
from src.core.idempotency import IdempotencyStore

store = IdempotencyStore(redis_client, namespace="mi-job", ttl_seconds=86400)

async def process_item(item_id: str) -> None:
    if not await store.claim(item_id):
        logger.info("item_already_processed", item_id=item_id)
        return
    # Procesar...
```

### Métodos disponibles

```python
# claim: marcar como procesado (SET NX). Retorna True si nuevo, False si duplicado.
claimed = await store.claim("evt-001")

# has_been_processed: consultar sin marcar (para debugging / tests).
processed = await store.has_been_processed("evt-001")

# release: borrar la marca (SOLO en tests o recovery manual).
await store.release("evt-001")
```

---

## TTL — cuánto tiempo retener

El TTL default es **7 días**. La lógica:

- Si un consumer tiene un rebalance y reprocesa mensajes, los eventos de los
  últimos N días quedarán marcados y serán detectados como duplicados.
- 7 días cubre la mayoría de los escenarios de reprocesamiento accidental.
- Keys que no expiran acumulan memoria en Redis indefinidamente.

Para casos con volumen muy alto (millones de eventos/día), considerar un TTL
menor y confiar en que los rebalances normales no tardan más de 24-48h.

Para casos que requieren reprocessing histórico más largo (ej. re-migrar
datos de 30 días), deshabilitar temporalmente la idempotencia o usar un
namespace alternativo:

```python
# Consumer de reprocesamiento con namespace distinto
consumer = MiConsumer(
    ...
    idempotency_namespace="mi-consumer-reprocess-nov2024",
    idempotency_ttl_seconds=90 * 24 * 3600,  # 90 días
)
```

---

## Keys en Redis

Para inspeccionar el estado:

```bash
# Ver todas las keys de idempotencia de un consumer
redis-cli keys "idempotency:example-consumer:*" | head -20

# Ver cuántas keys hay
redis-cli keys "idempotency:example-consumer:*" | wc -l

# Verificar si un evento específico está marcado
redis-cli exists "idempotency:example-consumer:evt-001"
# Retorna 1 si existe (procesado), 0 si no

# Ver TTL restante de una key
redis-cli ttl "idempotency:example-consumer:evt-001"
```

---

## Tests

En tests de integración, el fixture `redis_client` hace `flushdb()` al terminar,
lo que limpia todas las keys de idempotencia automáticamente.

```python
async def test_idempotency_prevents_double_processing(redis_client):
    store = IdempotencyStore(redis_client, namespace="test-consumer")

    # Primer claim: debe procesar
    assert await store.claim("evt-001") is True

    # Segundo claim del mismo evento: debe saltar
    assert await store.claim("evt-001") is False

    # Confirmar que está marcado
    assert await store.has_been_processed("evt-001") is True

async def test_release_allows_reprocessing(redis_client):
    store = IdempotencyStore(redis_client, namespace="test-consumer")
    await store.claim("evt-002")

    # Liberar la marca
    await store.release("evt-002")

    # Ahora puede procesarse de nuevo
    assert await store.claim("evt-002") is True
```

---

## Métricas

```
kafka_idempotency_duplicates_total{consumer="example-consumer", topic="example-events"} 47
```

Un número elevado indica:
- Rebalances frecuentes (consumer group inestable)
- Reprocesamiento masivo de un offset pasado
- Publicador enviando el mismo evento múltiples veces (bug upstream)

Un número en 0 durante semanas puede indicar que los eventos no traen `event_id`
y se están usando IDs sintéticos (topic-partition-offset) que no detectan
re-publicaciones.
