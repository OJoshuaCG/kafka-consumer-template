"""Factory para clientes Kafka/Redpanda con defaults de producción.

Producer y Consumer con configuración explícita anti-pie-en-la-cara:
- `enable_auto_commit=False` SIEMPRE — el BaseConsumer commitea manual.
- `acks=all` para producer (DLQ no se pierde nunca).
- `max_poll_interval_ms` generoso para handlers lentos.
"""

from __future__ import annotations

import ssl
from typing import Any

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.helpers import create_ssl_context


class KafkaClientFactory:
    """Construye AIOKafkaConsumer/Producer con defaults sanos.

    Args:
        bootstrap_servers: hosts de Kafka/Redpanda separados por coma.
        security_protocol: "PLAINTEXT" (dev) o "SASL_SSL" (prod).
        sasl_username/sasl_password: solo si security_protocol="SASL_SSL".
    """

    def __init__(
        self,
        bootstrap_servers: str,
        *,
        security_protocol: str = "PLAINTEXT",
        sasl_username: str | None = None,
        sasl_password: str | None = None,
    ) -> None:
        self._bootstrap = bootstrap_servers
        self._protocol = security_protocol
        self._sasl_username = sasl_username
        self._sasl_password = sasl_password

    def _auth_kwargs(self) -> dict[str, Any]:
        if self._protocol == "PLAINTEXT":
            return {"security_protocol": "PLAINTEXT"}
        if self._protocol == "SASL_SSL":
            ssl_ctx: ssl.SSLContext = create_ssl_context()
            return {
                "security_protocol": "SASL_SSL",
                "sasl_mechanism": "SCRAM-SHA-256",
                "sasl_plain_username": self._sasl_username,
                "sasl_plain_password": self._sasl_password,
                "ssl_context": ssl_ctx,
            }
        raise ValueError(f"Unsupported security_protocol: {self._protocol}")

    def consumer(
        self,
        topic: str,
        *,
        group_id: str,
        max_poll_interval_ms: int = 600_000,  # 10 min — generoso para handlers lentos
        session_timeout_ms: int = 30_000,
        heartbeat_interval_ms: int = 10_000,
        max_poll_records: int = 100,
    ) -> AIOKafkaConsumer:
        """Crea un AIOKafkaConsumer con `enable_auto_commit=False`."""
        return AIOKafkaConsumer(
            topic,
            bootstrap_servers=self._bootstrap,
            group_id=group_id,
            enable_auto_commit=False,       # el BaseConsumer commitea manual
            auto_offset_reset="earliest",
            max_poll_interval_ms=max_poll_interval_ms,
            session_timeout_ms=session_timeout_ms,
            heartbeat_interval_ms=heartbeat_interval_ms,
            max_poll_records=max_poll_records,
            **self._auth_kwargs(),
        )

    def producer(
        self,
        *,
        acks: str = "all",
        compression_type: str = "gzip",
        linger_ms: int = 50,
    ) -> AIOKafkaProducer:
        """Crea un AIOKafkaProducer con `acks=all` (durabilidad máxima)."""
        return AIOKafkaProducer(
            bootstrap_servers=self._bootstrap,
            acks=acks,
            compression_type=compression_type,
            linger_ms=linger_ms,
            enable_idempotence=True,        # exactly-once en producer-side
            **self._auth_kwargs(),
        )
