"""Métricas específicas del example consumer.

Las métricas BASE (MESSAGES_TOTAL, MESSAGE_DURATION, DLQ_TOTAL, etc) viven
en `src/core/metrics.py` y aplican automáticamente. Acá van métricas
específicas del dominio que NO tienen sentido a nivel framework.
"""

from __future__ import annotations

from prometheus_client import Counter

GREETINGS_PROCESSED = Counter(
    "example_greetings_processed_total",
    "Saludos procesados exitosamente",
)

FAREWELLS_PROCESSED = Counter(
    "example_farewells_processed_total",
    "Despedidas procesadas exitosamente",
)
