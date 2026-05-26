# Pattern: Background tasks para trabajo largo

> **Cuándo usarlo**: cuando `process_message` tomaría > 30 segundos.
> Bulk processing, llamadas a APIs externas con miles de items, ML inference batch.

## El problema

Kafka necesita que tu consumer haga `poll()` regularmente — si no, el broker
piensa que estás muerto, dispara rebalance, las particiones se reasignan,
y los mensajes en vuelo se reprocessan.

`max_poll_interval_ms` por default es 5 minutos en aiokafka. Si tu handler
tarda más, te pasa esto:

```
[Kafka] Consumer falló heartbeat → rebalance
[App]   Handler sigue corriendo, intenta commit → InvalidOffset
[App]   Mismo evento se procesa en otra réplica
```

## La solución

El BaseConsumer expone un hook opcional: `process_message_background()`.

```python
class BulkProcessor(BaseConsumer):
    async def process_message_background(self, event, raw_message):
        # 1. Persistir SIEMPRE en DB con status='processing'.
        #    Esto es lo que hace que el evento sea durable.
        await self._db.execute(
            "INSERT INTO bulk_jobs (event_id, status) VALUES ($1, 'processing') "
            "ON CONFLICT (event_id) DO UPDATE SET status='processing'",
            event["event_id"],
        )

        # 2. El BaseConsumer ya commiteó el offset por nosotros — el evento
        #    está safe en DB. Si crasheamos ahora, on_start() lo retoma.

        # 3. Hacer el trabajo pesado.
        try:
            await self._do_bulk_work(event)
            await self._db.execute(
                "UPDATE bulk_jobs SET status='done' WHERE event_id=$1",
                event["event_id"],
            )
        except Exception as exc:
            await self._db.execute(
                "UPDATE bulk_jobs SET status='failed', error=$2 WHERE event_id=$1",
                event["event_id"], str(exc),
            )
            raise

    async def on_start(self):
        await super().on_start()
        # 4. CRASH RECOVERY: buscar jobs que quedaron en 'processing'
        #    y retomarlos.
        pending = await self._db.fetch_all(
            "SELECT * FROM bulk_jobs WHERE status='processing'",
        )
        for row in pending:
            logger.warning("recovering_pending_job", event_id=row["event_id"])
            asyncio.create_task(self._do_bulk_work(dict(row)))
```

## Qué hace el BaseConsumer automáticamente

Cuando detecta que overrideaste `process_message_background()`:

1. Llama a tu método.
2. **Inmediatamente** commitea el offset del mensaje en Kafka.
3. Trackea el task en `_background_tasks` para shutdown limpio.
4. Actualiza `BACKGROUND_TASKS_PENDING` Prometheus gauge.
5. En `_shutdown()`, espera tasks pendientes con timeout de 30s.

## Reglas duras

- **Persistir ANTES de hacer trabajo I/O**. Si no, perdés durabilidad. El
  offset ya se commiteó.
- **Implementar `on_start()` con crash recovery** que busque registros
  `status='processing'`. Sin esto, si el pod muere mientras hay un task
  corriendo, ese trabajo se pierde silenciosamente.
- **Marcar `status='done'` / `status='failed'` al terminar**. Si no, el crash
  recovery va a reintentar tu task aunque haya terminado.
- **NO** mezclar sync y async pattern en el mismo consumer. Es uno o el otro.

## Cuándo NO usarlo

Si tu handler tarda < 30s, usá `process_message()` sync. Más simple,
menos cosas que pueden fallar, no requiere tabla auxiliar.

> "Background tasks son útiles cuando los necesitás. Cuando no los necesitás,
> son una fuente de bugs."

## Métricas a watch

- `kafka_background_tasks_pending{consumer="..."}` — debería bajar a 0 entre
  ráfagas. Si crece monotónicamente, hay un leak.
- Queries: `SELECT status, COUNT(*) FROM bulk_jobs GROUP BY status` — debería
  estar dominado por `done`. Mucho `processing` después de un rato = tasks
  zombi.
