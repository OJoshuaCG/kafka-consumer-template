"""Fixtures globales compartidas — unit + integration."""

from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _reset_settings_cache() -> None:
    """Resetea los `@lru_cache` de settings entre tests."""
    from src.config.settings import get_global_settings
    from src.consumers.example.settings import get_example_settings

    get_global_settings.cache_clear()
    get_example_settings.cache_clear()
