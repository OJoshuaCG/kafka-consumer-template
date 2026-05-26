"""Structured logging con structlog — JSON en prod, console legible en dev.

Inspirado en `python-kafka-consumers/src/utils/logging.py`:
- ProductionJSONRenderer reordena campos para legibilidad en Loki/Datadog.
- ContextVars (`current_message_id`, etc) se incluyen automáticamente como
  processors, sin tener que pasarlos por parámetro.
"""

from __future__ import annotations

import logging
import sys
from typing import Any

import orjson
import structlog

from src.core.context import context_snapshot


def _inject_context_vars(
    _logger: object,
    _method_name: str,
    event_dict: dict[str, Any],
) -> dict[str, Any]:
    """Inyecta los ContextVars del consumer en cada log line."""
    snapshot = {k: v for k, v in context_snapshot().items() if v is not None}
    event_dict.update(snapshot)
    return event_dict


def _orjson_dumps(obj: Any, default: Any = None) -> str:
    return orjson.dumps(obj, default=default).decode()


class ProductionJSONRenderer:
    """JSON renderer con orden de campos fijo.

    El ojo escanea las primeras keys del JSON cuando filtrás logs. Poner
    `timestamp/level/event/environment` primero hace que cada log line sea
    legible de un vistazo en Datadog/Loki.
    """

    PRIORITY_FIELDS = ("timestamp", "level", "event", "environment")

    def __call__(
        self,
        _logger: object,
        _name: str,
        event_dict: dict[str, Any],
    ) -> str:
        ordered: dict[str, Any] = {}
        for key in self.PRIORITY_FIELDS:
            if key in event_dict:
                ordered[key] = event_dict.pop(key)
        ordered.update(event_dict)
        return _orjson_dumps(ordered)


def setup_logging(
    *,
    level: str = "INFO",
    environment: str = "development",
    json_output: bool | None = None,
) -> None:
    """Configura structlog globalmente.

    Args:
        level: nivel mínimo de logging.
        environment: se incluye como field en cada log.
        json_output: forzar JSON (True) o console (False). Default: JSON si
            environment != 'development'.
    """
    use_json = json_output if json_output is not None else environment != "development"

    shared_processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        _inject_context_vars,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    # `environment` siempre presente
    shared_processors.append(
        lambda _l, _m, ed: {**ed, "environment": environment},
    )

    renderer: Any = (
        ProductionJSONRenderer()
        if use_json
        else structlog.dev.ConsoleRenderer(colors=True)
    )

    structlog.configure(
        processors=[*shared_processors, renderer],
        wrapper_class=structlog.make_filtering_bound_logger(getattr(logging, level.upper())),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(file=sys.stdout),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str) -> Any:
    """Devuelve un logger structlog para el módulo `name`.

    Convención: `logger = get_logger(__name__)` en cada archivo, no
    singleton global. Permite filtros y niveles por módulo.
    """
    return structlog.get_logger(name)
