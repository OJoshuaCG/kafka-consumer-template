# Crear un nuevo consumer

Workflow real, paso a paso. El template está diseñado para que **copiar
el folder del example sea el camino feliz**.

## 1. Copiar el example

```bash
cp -r src/consumers/example/ src/consumers/whatsapp/
```

## 2. Adaptar `settings.py`

Cambiar el `env_prefix` y los defaults. Un consumer = un prefijo único.

```python
# src/consumers/whatsapp/settings.py
class WhatsAppConsumerSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="WHATSAPP_",       # ← prefijo único
        extra="ignore",
    )

    topic: str = Field("whatsapp-events")
    group_id: str = Field("whatsapp-consumer")
    dlq_topic: str = Field("whatsapp-events-dlq")
    ...

@lru_cache
def get_whatsapp_settings() -> WhatsAppConsumerSettings:
    return WhatsAppConsumerSettings()
```

## 3. Definir los schemas

Pydantic v2 con `Literal` discriminator si hay varios tipos de eventos:

```python
# src/consumers/whatsapp/schemas.py
class MessageReceivedEvent(BaseModel):
    type: Literal["message_received"]
    event_id: str
    from_number: str
    body: str

class MessageDeliveredEvent(BaseModel):
    type: Literal["message_delivered"]
    event_id: str
    message_id: str

WhatsAppEvent = Annotated[
    MessageReceivedEvent | MessageDeliveredEvent,
    Field(discriminator="type"),
]
```

## 4. Implementar los handlers

UNA función async PURA por tipo de evento. Sin Kafka, sin commit, sin retry.

```python
# src/consumers/whatsapp/handlers.py
async def handle_message_received(event: MessageReceivedEvent, db: Database) -> None:
    if not is_valid_number(event.from_number):
        raise NonRetryableError("Invalid phone number", context={"number": event.from_number})

    await db.execute(
        "INSERT INTO messages (event_id, from_number, body) VALUES ($1, $2, $3)",
        event.event_id, event.from_number, event.body,
    )
```

**Reglas**:
- Validación de dominio falla → `NonRetryableError`
- Red caída, lock, timeout → `RetryableError`
- Si NO hay validación que justifique los errores, simplemente dejá propagar
  cualquier excepción — el BaseConsumer la atrapará como "unhandled" y mandará
  a DLQ.

## 5. Adaptar el consumer

```python
# src/consumers/whatsapp/consumer.py
class WhatsAppConsumer(BaseConsumer):
    name = "whatsapp-consumer"

    def __init__(self, *, db: Database, **kwargs):
        super().__init__(**kwargs)
        self._db = db

    async def on_start(self) -> None:
        await self._db.connect()

    async def on_stop(self) -> None:
        await self._db.close()

    async def process_message(self, event, raw_message):
        parsed = _adapter.validate_python(event)
        if isinstance(parsed, MessageReceivedEvent):
            await handle_message_received(parsed, self._db)
        elif isinstance(parsed, MessageDeliveredEvent):
            await handle_message_delivered(parsed, self._db)
```

## 6. Registrar entry point

En `pyproject.toml`:

```toml
[project.scripts]
example-consumer = "src.consumers.example.consumer:run"
whatsapp-consumer = "src.consumers.whatsapp.consumer:run"   # ← nuevo
```

Después:

```bash
uv sync
uv run whatsapp-consumer
```

## 7. Variables de entorno

Agregar al `.env`:

```bash
WHATSAPP_TOPIC=whatsapp-events
WHATSAPP_GROUP_ID=whatsapp-consumer
WHATSAPP_DLQ_TOPIC=whatsapp-events-dlq
```

## 8. Tests

- `tests/unit/consumers/whatsapp/test_handlers.py` — testear handlers con `FakeDB`.
- `tests/integration/consumers/whatsapp/test_consumer.py` — flujo completo con Testcontainers.

## 9. K8s deployment

```bash
cp k8s/deployment.yaml k8s/whatsapp-deployment.yaml
# Cambiar: metadata.name, labels app, container.name, container.image,
#         container.command (apunta a whatsapp-consumer), env vars con prefijo WHATSAPP_.
```

## 10. (Opcional) Background tasks

Si el handler tarda > 30s, ver [`patterns/background-tasks.md`](patterns/background-tasks.md)
para overridear `process_message_background()` en vez de `process_message()`.
