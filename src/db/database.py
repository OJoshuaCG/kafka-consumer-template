"""Async DB wrapper (asyncpg) con retry, backoff+jitter y bulk insert.

Diseñado para los patterns típicos de consumers:
- `execute()` — queries simples
- `fetch_one()` / `fetch_all()` — SELECTs
- `call_procedure()` — stored procedures
- `insert_batch()` — bulk insert multi-VALUES (mucho más rápido que N inserts)

Nombres de tablas/procedures dinámicos pasan por `validate_sql_identifier`
para prevenir SQL injection. Parámetros SIEMPRE como placeholders.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, cast

import asyncpg

from src.core.exceptions import RetryableError
from src.core.logging import get_logger
from src.core.retry import retry_async
from src.core.utils import validate_sql_identifier

logger = get_logger(__name__)


# Errores asyncpg que merecen retry — el resto se levanta tal cual.
_RETRYABLE_PG_ERRORS: tuple[type[Exception], ...] = (
    asyncpg.exceptions.DeadlockDetectedError,
    asyncpg.exceptions.SerializationError,
    asyncpg.exceptions.ConnectionDoesNotExistError,
    asyncpg.exceptions.InterfaceError,
)


class Database:
    """Wrapper async sobre asyncpg con retry + bulk insert.

    Mantiene un pool. Llamar `connect()` al inicio del consumer y
    `close()` en `on_stop()`.
    """

    def __init__(
        self,
        dsn: str,
        *,
        min_size: int = 2,
        max_size: int = 10,
        command_timeout: float = 30.0,
        max_retries: int = 3,
        retry_base_delay: float = 0.5,
    ) -> None:
        self._dsn = dsn
        self._min_size = min_size
        self._max_size = max_size
        self._command_timeout = command_timeout
        self._max_retries = max_retries
        self._retry_base_delay = retry_base_delay
        self._pool: asyncpg.Pool[asyncpg.Record] | None = None

    async def connect(self) -> None:
        self._pool = await asyncpg.create_pool(
            self._dsn,
            min_size=self._min_size,
            max_size=self._max_size,
            command_timeout=self._command_timeout,
        )
        logger.info("db_pool_ready", min_size=self._min_size, max_size=self._max_size)

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    @property
    def pool(self) -> asyncpg.Pool[asyncpg.Record]:
        if self._pool is None:
            raise RuntimeError("Database not connected — call connect() first")
        return self._pool

    # =============================================================
    # Operaciones básicas con retry transparente
    # =============================================================
    async def execute(self, query: str, *args: Any) -> str:
        async def _op() -> str:
            async with self.pool.acquire() as conn:
                try:
                    return cast(str, await conn.execute(query, *args))
                except _RETRYABLE_PG_ERRORS as exc:
                    raise RetryableError(f"PG transient error: {exc}") from exc

        return await retry_async(
            _op, max_attempts=self._max_retries, base_delay=self._retry_base_delay,
        )

    async def fetch_one(self, query: str, *args: Any) -> asyncpg.Record | None:
        async def _op() -> asyncpg.Record | None:
            async with self.pool.acquire() as conn:
                try:
                    return await conn.fetchrow(query, *args)
                except _RETRYABLE_PG_ERRORS as exc:
                    raise RetryableError(f"PG transient error: {exc}") from exc

        return await retry_async(
            _op, max_attempts=self._max_retries, base_delay=self._retry_base_delay,
        )

    async def fetch_all(self, query: str, *args: Any) -> list[asyncpg.Record]:
        async def _op() -> list[asyncpg.Record]:
            async with self.pool.acquire() as conn:
                try:
                    return cast(list[asyncpg.Record], await conn.fetch(query, *args))
                except _RETRYABLE_PG_ERRORS as exc:
                    raise RetryableError(f"PG transient error: {exc}") from exc

        return await retry_async(
            _op, max_attempts=self._max_retries, base_delay=self._retry_base_delay,
        )

    # =============================================================
    # Stored procedures — nombre validado con regex
    # =============================================================
    async def call_procedure(self, name: str, *args: Any) -> list[asyncpg.Record]:
        """Llama un stored procedure por nombre. Valida el identificador."""
        safe_name = validate_sql_identifier(name, kind="procedure")
        placeholders = ", ".join(f"${i}" for i in range(1, len(args) + 1))
        query = f"CALL {safe_name}({placeholders})"
        return await self.fetch_all(query, *args)

    # =============================================================
    # Bulk insert multi-VALUES — UNA query, no N
    # =============================================================
    async def insert_batch(
        self,
        table: str,
        columns: Sequence[str],
        rows: Sequence[Sequence[Any]],
    ) -> int:
        """Insert múltiples filas en una sola query.

        Mucho más eficiente que N inserts individuales — una sola network
        roundtrip, un solo plan de query.

        Args:
            table: nombre de la tabla (validado con regex).
            columns: lista de nombres de columna (cada uno validado).
            rows: lista de tuplas con los valores. TODAS las tuplas deben
                tener el mismo largo que `columns`.

        Returns:
            Número de filas insertadas.
        """
        if not rows:
            return 0

        safe_table = validate_sql_identifier(table, kind="table")
        safe_columns = [validate_sql_identifier(c, kind="column") for c in columns]
        n_cols = len(safe_columns)

        for i, row in enumerate(rows):
            if len(row) != n_cols:
                raise ValueError(
                    f"Row {i} has {len(row)} values but {n_cols} columns expected",
                )

        # Construir placeholders: ($1, $2, ..., $n), ($n+1, ..., $2n), ...
        placeholders_per_row: list[str] = []
        flat_args: list[Any] = []
        idx = 1
        for row in rows:
            row_placeholders = ", ".join(f"${i}" for i in range(idx, idx + n_cols))
            placeholders_per_row.append(f"({row_placeholders})")
            flat_args.extend(row)
            idx += n_cols

        cols_sql = ", ".join(safe_columns)
        values_sql = ", ".join(placeholders_per_row)
        query = f"INSERT INTO {safe_table} ({cols_sql}) VALUES {values_sql}"

        await self.execute(query, *flat_args)
        return len(rows)
