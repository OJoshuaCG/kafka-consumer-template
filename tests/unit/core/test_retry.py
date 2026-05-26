"""Tests del backoff con jitter y retry_async."""

from __future__ import annotations

import pytest

from src.core.exceptions import NonRetryableError, RetryableError
from src.core.retry import backoff_with_jitter, retry_async


class TestBackoffWithJitter:
    def test_jitter_is_within_bounds(self) -> None:
        # base * 2^attempt * (0.5..1.5) ⇒ entre 0.5x y 1.5x del base
        for attempt in range(5):
            for _ in range(50):
                delay = backoff_with_jitter(attempt, base=1.0, cap=1000.0)
                expected_min = 1.0 * (2**attempt) * 0.5
                expected_max = 1.0 * (2**attempt) * 1.5
                assert expected_min <= delay <= expected_max

    def test_respects_cap(self) -> None:
        delay = backoff_with_jitter(attempt=20, base=1.0, cap=10.0)
        assert delay <= 10.0

    def test_grows_exponentially(self) -> None:
        # Promedio de muchas muestras debería crecer ~2x por attempt
        samples_0 = [backoff_with_jitter(0, base=1.0, cap=1000.0) for _ in range(100)]
        samples_3 = [backoff_with_jitter(3, base=1.0, cap=1000.0) for _ in range(100)]
        avg_0 = sum(samples_0) / len(samples_0)
        avg_3 = sum(samples_3) / len(samples_3)
        assert avg_3 > avg_0 * 4  # 2^3 = 8x, dejando margen para jitter


class TestRetryAsync:
    async def test_returns_on_first_success(self) -> None:
        calls = 0

        async def op() -> str:
            nonlocal calls
            calls += 1
            return "ok"

        result = await retry_async(op, max_attempts=3, base_delay=0.001)
        assert result == "ok"
        assert calls == 1

    async def test_retries_on_retryable_then_succeeds(self) -> None:
        calls = 0

        async def op() -> str:
            nonlocal calls
            calls += 1
            if calls < 3:
                raise RetryableError("transient")
            return "ok"

        result = await retry_async(op, max_attempts=5, base_delay=0.001)
        assert result == "ok"
        assert calls == 3

    async def test_reraises_after_max_attempts(self) -> None:
        async def op() -> str:
            raise RetryableError("always fails")

        with pytest.raises(RetryableError, match="always fails"):
            await retry_async(op, max_attempts=3, base_delay=0.001)

    async def test_does_not_retry_unspecified_errors(self) -> None:
        calls = 0

        async def op() -> str:
            nonlocal calls
            calls += 1
            raise NonRetryableError("permanent")

        with pytest.raises(NonRetryableError):
            await retry_async(op, max_attempts=3, base_delay=0.001)
        assert calls == 1
