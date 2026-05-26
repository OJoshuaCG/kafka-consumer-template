"""Settings del example consumer — env prefix `EXAMPLE_`.

Para agregar un nuevo consumer, COPIAR este archivo a
`src/consumers/<nuevo>/settings.py` y cambiar:
- el `env_prefix` (ej. `WHATSAPP_`)
- los defaults
"""

from __future__ import annotations

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class ExampleConsumerSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="EXAMPLE_",
        extra="ignore",
    )

    topic: str = Field("example-events")
    group_id: str = Field("example-consumer")
    dlq_topic: str = Field("example-events-dlq")
    batch_size: int = Field(100)
    max_retries: int = Field(3)


@lru_cache
def get_example_settings() -> ExampleConsumerSettings:
    return ExampleConsumerSettings()
