# Pattern: Concurrencia con fair scheduling

> **Cuándo usarlo**: cuando un consumer procesa eventos heterogéneos en cuanto
> a duración (ej. campañas de WhatsApp que pueden tener 10 o 100.000 destinatarios)
> y querés evitar que las grandes monopolicen los slots de concurrencia.

## El problema

`asyncio.Semaphore` provee concurrencia limitada pero **no es fair**: si
tenés 3 slots y arrancan 3 tasks que tardan 1 hora cada uno, ningún task
nuevo entra hasta que UNO termine. Tasks chicos que entran después esperan
detrás de los grandes, aunque solo tardarían segundos.

## La solución: cooperative yielding

El task largo cede voluntariamente su slot si hay otros esperando, retoma
el trabajo cuando vuelve a haber espacio.

```python
class FairSemaphore:
    def __init__(self, value: int):
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
        """Si hay tasks esperando, libera el slot y lo re-adquiere al final de la cola."""
        if self._waiters > 0:
            self.release()
            await asyncio.sleep(0)   # ceder al event loop
            await self.acquire()
```

Uso desde un handler largo:

```python
async def process_campaign(campaign_id: str, db, fair_sem: FairSemaphore):
    await fair_sem.acquire()
    try:
        recipients = await db.fetch_all(...)
        for batch in chunks(recipients, 100):
            await send_batch(batch)
            await fair_sem.yield_if_others_waiting()  # ← ceder entre lotes
    finally:
        fair_sem.release()
```

## Reglas

- Solo aplica cuando el task tiene **puntos naturales de cesión** (entre
  lotes, entre iteraciones de un loop). No funciona si el trabajo es un solo
  `await` largo (ej. una query que tarda 30s).
- La métrica `_waiters` no es perfectamente atómica en Python, pero es
  "good enough" porque el GIL serializa las modificaciones.
- Combinarlo con `process_message_background()` (ver [`background-tasks.md`](background-tasks.md))
  si el trabajo total tarda > 30s.

## Métrica útil

```python
WAIT_TIME = Histogram(
    "fair_semaphore_wait_seconds",
    "Tiempo que un task esperó por un slot",
    labelnames=("name",),
)
```

Una distribución larga en la cola = considerar más slots o reducir el trabajo
por task.
