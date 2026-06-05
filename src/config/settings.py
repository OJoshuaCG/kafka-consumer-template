"""Settings globales — única fuente de verdad para env vars compartidas.

Per-consumer settings viven en `src/consumers/<name>/settings.py` y usan SU
propio `env_prefix` para evitar colisiones. Ver `docs/creating-a-consumer.md`.
"""

from __future__ import annotations

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class GlobalSettings(BaseSettings):
    """Variables compartidas por TODO el proyecto."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="",
        extra="ignore",
    )

    # App
    environment: str = Field("development", description="development|staging|production")
    log_level: str = Field("INFO", description="DEBUG|INFO|WARNING|ERROR|CRITICAL")

    # Kafka
    kafka_bootstrap_servers: str = Field("localhost:9092")
    kafka_security_protocol: str = Field("PLAINTEXT")
    kafka_sasl_username: str | None = None
    kafka_sasl_password: str | None = None

    # DB
    database_url: str = Field("mysql://kafka:kafka@localhost:3306/kafka_consumer")

    # Redis
    redis_url: str = Field("redis://localhost:6379/0")

    # Métricas
    metrics_port: int = Field(9090)
    metrics_enabled: bool = Field(True)

    # Health
    health_file_path: str = Field("/tmp/healthcheck")
    health_write_interval_seconds: float = Field(10.0)


@lru_cache
def get_global_settings() -> GlobalSettings:
    """Singleton cacheado de las settings globales.

    Usar `@lru_cache` garantiza una sola instancia por proceso. En tests,
    llamar `get_global_settings.cache_clear()` para resetear.
    """
    return GlobalSettings()
