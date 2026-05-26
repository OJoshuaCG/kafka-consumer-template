"""ContextVars para propagar trace fields a todos los logs sin pasarlos por parámetro.

El BaseConsumer setea estas variables al recibir un mensaje. Cualquier
`logger.info()` debajo en el call stack las hereda automáticamente.
"""

from contextvars import ContextVar

current_message_id: ContextVar[str | None] = ContextVar("current_message_id", default=None)
current_consumer_name: ContextVar[str | None] = ContextVar("current_consumer_name", default=None)
current_topic: ContextVar[str | None] = ContextVar("current_topic", default=None)
current_event_type: ContextVar[str | None] = ContextVar("current_event_type", default=None)
current_attempt: ContextVar[int] = ContextVar("current_attempt", default=0)


def context_snapshot() -> dict[str, object]:
    """Snapshot de todas las ContextVars actuales — para logging y debugging."""
    return {
        "message_id": current_message_id.get(),
        "consumer_name": current_consumer_name.get(),
        "topic": current_topic.get(),
        "event_type": current_event_type.get(),
        "attempt": current_attempt.get(),
    }
