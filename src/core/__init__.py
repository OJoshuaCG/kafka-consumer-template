"""Framework Kafka — no tocar para nueva lógica de negocio.

Re-exports los símbolos públicos del core para imports más cortos:

    from src.core import BaseConsumer, RetryableError, get_logger
"""

from src.core.consumer import BaseConsumer, ConsumerState
from src.core.context import (
    current_attempt,
    current_consumer_name,
    current_event_type,
    current_message_id,
    current_topic,
)
from src.core.exceptions import DomainError, NonRetryableError, RetryableError
from src.core.logging import get_logger, setup_logging
from src.core.retry import backoff_with_jitter

__all__ = [
    "BaseConsumer",
    "ConsumerState",
    "DomainError",
    "NonRetryableError",
    "RetryableError",
    "backoff_with_jitter",
    "current_attempt",
    "current_consumer_name",
    "current_event_type",
    "current_message_id",
    "current_topic",
    "get_logger",
    "setup_logging",
]
