"""Tests de los handlers del example consumer.

Demuestra cómo testear handlers como funciones puras: con un mock de la
DB que registra las llamadas, sin Redpanda, sin Redis, sin Postgres real.
Los integration tests que SÍ levantan infra viven en tests/integration/.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest

from src.consumers.example.handlers import handle_farewell, handle_greeting
from src.consumers.example.schemas import FarewellEvent, GreetingEvent
from src.core.exceptions import NonRetryableError


@dataclass
class FakeDB:
    """Mock mínimo de Database — captura las llamadas a `execute`."""

    calls: list[tuple[str, tuple[Any, ...]]] = field(default_factory=list)

    async def execute(self, query: str, *args: Any) -> str:
        self.calls.append((query, args))
        return "INSERT 0 1"


class TestHandleGreeting:
    async def test_persists_valid_event(self) -> None:
        db = FakeDB()
        event = GreetingEvent(
            type="greeting",
            event_id="evt-1",
            user_id="user-1",
            message="hola",
        )
        await handle_greeting(event, db)  # type: ignore[arg-type]

        assert len(db.calls) == 1
        query, args = db.calls[0]
        assert "greetings" in query
        assert args == ("evt-1", "user-1", "hola")

    async def test_rejects_empty_message(self) -> None:
        db = FakeDB()
        event = GreetingEvent(
            type="greeting",
            event_id="evt-1",
            user_id="user-1",
            message="   ",
        )
        with pytest.raises(NonRetryableError, match="vacío"):
            await handle_greeting(event, db)  # type: ignore[arg-type]
        assert db.calls == []


class TestHandleFarewell:
    async def test_persists_with_reason(self) -> None:
        db = FakeDB()
        event = FarewellEvent(
            type="farewell",
            event_id="evt-2",
            user_id="user-2",
            reason="explicit logout",
        )
        await handle_farewell(event, db)  # type: ignore[arg-type]
        assert db.calls[0][1] == ("evt-2", "user-2", "explicit logout")

    async def test_persists_without_reason(self) -> None:
        db = FakeDB()
        event = FarewellEvent(
            type="farewell",
            event_id="evt-3",
            user_id="user-3",
            reason=None,
        )
        await handle_farewell(event, db)  # type: ignore[arg-type]
        assert db.calls[0][1] == ("evt-3", "user-3", None)
