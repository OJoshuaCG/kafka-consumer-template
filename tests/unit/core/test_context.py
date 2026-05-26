"""Tests de ContextVars del consumer."""

from __future__ import annotations

import asyncio

from src.core.context import (
    context_snapshot,
    current_attempt,
    current_consumer_name,
    current_event_type,
    current_message_id,
    current_topic,
)


class TestContextVars:
    def test_defaults(self) -> None:
        assert current_message_id.get() is None
        assert current_consumer_name.get() is None
        assert current_topic.get() is None
        assert current_event_type.get() is None
        assert current_attempt.get() == 0

    def test_snapshot(self) -> None:
        t1 = current_message_id.set("msg-123")
        t2 = current_consumer_name.set("example")
        t3 = current_topic.set("test-topic")
        try:
            snap = context_snapshot()
            assert snap["message_id"] == "msg-123"
            assert snap["consumer_name"] == "example"
            assert snap["topic"] == "test-topic"
            assert snap["attempt"] == 0
        finally:
            current_message_id.reset(t1)
            current_consumer_name.reset(t2)
            current_topic.reset(t3)

    async def test_isolation_across_tasks(self) -> None:
        """Cada task tiene su propio set de ContextVars."""

        async def task_a() -> str | None:
            current_message_id.set("msg-A")
            await asyncio.sleep(0.01)
            return current_message_id.get()

        async def task_b() -> str | None:
            current_message_id.set("msg-B")
            await asyncio.sleep(0.01)
            return current_message_id.get()

        results = await asyncio.gather(task_a(), task_b())
        assert results == ["msg-A", "msg-B"]
