"""Producer demo — FastAPI para disparar eventos manualmente durante desarrollo.

NUNCA va a producción. Lo prueba la separación de dependency-groups:
FastAPI vive en [dependency-groups] dev, no en [project.dependencies].

Levantar con:
    uv run uvicorn tools.producer_demo.main:app --reload
    # → http://localhost:8000/docs
"""

from __future__ import annotations

import os
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated, Literal

import orjson
from aiokafka import AIOKafkaProducer
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

# Lazy producer — se inicia en el lifespan
_producer: AIOKafkaProducer | None = None


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    global _producer
    _producer = AIOKafkaProducer(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        acks="all",
        enable_idempotence=True,
    )
    await _producer.start()
    yield
    if _producer is not None:
        await _producer.stop()
        _producer = None


app = FastAPI(
    title="Producer Demo",
    description="Dev tool para disparar eventos hacia Redpanda local. NUNCA usar en producción.",
    version="0.1.0",
    lifespan=lifespan,
)


class GreetingPayload(BaseModel):
    type: Literal["greeting"] = "greeting"
    user_id: str = Field(..., examples=["user-123"])
    message: str = Field(..., examples=["Hola mundo"])


class FarewellPayload(BaseModel):
    type: Literal["farewell"] = "farewell"
    user_id: str = Field(..., examples=["user-123"])
    reason: str | None = Field(None, examples=["explicit logout"])


Payload = Annotated[GreetingPayload | FarewellPayload, Field(discriminator="type")]


@app.post("/publish/example")
async def publish_example(
    payload: Payload,
    topic: str = "example-events",
) -> dict[str, str]:
    """Publica un evento al topic del example consumer.

    Genera `event_id` automáticamente si no se provee.
    """
    if _producer is None:
        raise HTTPException(503, "Producer not ready")

    event = payload.model_dump()
    event["event_id"] = str(uuid.uuid4())

    await _producer.send_and_wait(
        topic,
        value=orjson.dumps(event),
        key=event["user_id"].encode(),
    )
    return {"status": "published", "topic": topic, "event_id": event["event_id"]}


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}
