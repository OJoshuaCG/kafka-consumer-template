"""Pydantic models para los eventos del example topic.

Un consumer típico tiene varios `event_type` distintos. Un BaseModel por
tipo de evento, todos discriminables por el field `type`.
"""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, Field


class GreetingEvent(BaseModel):
    """Evento de saludo — `type='greeting'`."""

    type: Literal["greeting"]
    event_id: str = Field(..., min_length=1)
    user_id: str = Field(..., min_length=1)
    message: str = Field(..., max_length=500)


class FarewellEvent(BaseModel):
    """Evento de despedida — `type='farewell'`."""

    type: Literal["farewell"]
    event_id: str = Field(..., min_length=1)
    user_id: str = Field(..., min_length=1)
    reason: str | None = Field(None, max_length=200)


# Union discriminada por el field `type` — Pydantic elige el modelo correcto
ExampleEvent = Annotated[GreetingEvent | FarewellEvent, Field(discriminator="type")]
