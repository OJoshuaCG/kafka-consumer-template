"""Async DB wrapper con soporte para MariaDB (aiomysql) y PostgreSQL (asyncpg).

API pública idéntica independientemente del motor. El engine se elige con
`create_database(dsn)` — el scheme del DSN determina la implementación:

    mysql://user:pass@host/db       → MariaDBDatabase   (aiomysql)
    postgresql://user:pass@host/db  → PostgreSQLDatabase (asyncpg)

Queries se escriben con %s como placeholder universal. PostgreSQL recibe
la conversión a $1, $2, ... de forma transparente.

Operaciones:
- execute()        — INSERT / UPDATE / DELETE
- fetch_one()      — SELECT fila única  → dict | None
- fetch_all()      — SELECT múltiples   → list[dict]
- call_procedure() — stored procedures
- insert_batch()   — bulk insert multi-VALUES (mucho más rápido que N inserts)

Nombres de tablas/procedures dinámicos pasan por `validate_sql_identifier`
para prevenir SQL injection. Parámetros SIEMPRE como placeholders (%s).
"""

from __future__ import annotations

import re
from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import Any
from urllib.parse import urlparse

from src.core.exceptions import RetryableError
from src.core.logging import get_logger
from src.core.retry import retry_async
from src.core.utils import validate_sql_identifier

logger = get_logger(__name__)


def _to_positional(query: str) -> str:
    """Convierte placeholders %s → $1, $2, ... para asyncpg.

    Sigue la semántica DB-API 2.0: %% se convierte a % literal,
    %s se convierte a $N. Cualquier otro %x se deja tal cual.
    """
    result: list[str] = []
    counter = 0
    i = 0
    while i < len(query):
        if query[i] == "%" and i + 1 < len(query):
            if query[i + 1] == "s":
                counter += 1
                result.append(f"${counter}")
                i += 2
            elif query[i + 1] == "%":
                result.append("%")
                i += 2
            else:
                result.append(query[i])
                i += 1
        else:
            result.append(query[i])
            i += 1
    return "".join(result)


# ─────────────────────────────────────────────────────────────────────────────
# Base abstracta — interfaz común para ambos motores
# ─────────────────────────────────────────────────────────────────────────────


class Database(ABC):
    """Wrapper async con retry + bulk insert. Motor elegido por subclase.

    Ciclo de vida:
        db = create_database(dsn)
        await db.connect()          # on_start() del consumer
        ...
        await db.close()            # on_stop() del consumer
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

    @abstractmethod
    async def connect(self) -> None: ...

    @abstractmethod
    async def close(self) -> None: ...

    @abstractmethod
    async def execute(self, query: str, *args: Any) -> str:
        """Ejecuta INSERT / UPDATE / DELETE. Retorna 'rowcount=N'."""
        ...

    @abstractmethod
    async def fetch_one(self, query: str, *args: Any) -> dict[str, Any] | None:
        """Retorna la primera fila o None."""
        ...

    @abstractmethod
    async def fetch_all(self, query: str, *args: Any) -> list[dict[str, Any]]:
        """Retorna todas las filas."""
        ...

    @abstractmethod
    async def call_procedure(self, name: str, *args: Any) -> list[dict[str, Any]]:
        """Llama un stored procedure por nombre validado."""
        ...

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
                    f"Row {i} has {len(row)} values but {n_cols} columns expected"
                )

        row_ph = f"({', '.join(['%s'] * n_cols)})"
        placeholders = ", ".join(row_ph for _ in rows)
        flat_args = [val for row in rows for val in row]

        cols_sql = ", ".join(safe_columns)
        query = f"INSERT INTO {safe_table} ({cols_sql}) VALUES {placeholders}"
        await self.execute(query, *flat_args)
        return len(rows)


# ─────────────────────────────────────────────────────────────────────────────
# MariaDB / MySQL — aiomysql
# ─────────────────────────────────────────────────────────────────────────────


class MariaDBDatabase(Database):
    """Implementación para MariaDB/MySQL usando aiomysql."""

    def __init__(self, dsn: str, **kwargs: Any) -> None:
        super().__init__(dsn, **kwargs)
        self._pool: Any = None
        self._retryable_errors: tuple[type[Exception], ...] = ()

    async def connect(self) -> None:
        import aiomysql

        # OperationalError: deadlock (1213), lost connection (2006/2013), can't connect (2003)
        # InternalError: deadlock detectado por InnoDB (1213)
        self._retryable_errors = (aiomysql.OperationalError, aiomysql.InternalError)

        parsed = urlparse(self._dsn)
        self._pool = await aiomysql.create_pool(
            host=parsed.hostname or "localhost",
            port=parsed.port or 3306,
            user=parsed.username or "root",
            password=parsed.password or "",
            db=parsed.path.lstrip("/"),
            minsize=self._min_size,
            maxsize=self._max_size,
            connect_timeout=self._command_timeout,
            charset="utf8mb4",
            cursorclass=aiomysql.DictCursor,
            autocommit=False,
        )
        logger.info("mariadb_pool_ready", min_size=self._min_size, max_size=self._max_size)

    async def close(self) -> None:
        if self._pool is not None:
            self._pool.close()
            await self._pool.wait_closed()
            self._pool = None

    async def execute(self, query: str, *args: Any) -> str:
        retryable = self._retryable_errors

        async def _op() -> str:
            async with self._pool.acquire() as conn:
                try:
                    async with conn.cursor() as cur:
                        await cur.execute(query, args or None)
                        await conn.commit()
                        return f"rowcount={cur.rowcount}"
                except retryable as exc:
                    raise RetryableError(f"MySQL transient error: {exc}") from exc

        return await retry_async(
            _op, max_attempts=self._max_retries, base_delay=self._retry_base_delay
        )

    async def fetch_one(self, query: str, *args: Any) -> dict[str, Any] | None:
        retryable = self._retryable_errors

        async def _op() -> dict[str, Any] | None:
            async with self._pool.acquire() as conn:
                try:
                    async with conn.cursor() as cur:
                        await cur.execute(query, args or None)
                        return await cur.fetchone()  # type: ignore[return-value]
                except retryable as exc:
                    raise RetryableError(f"MySQL transient error: {exc}") from exc

        return await retry_async(
            _op, max_attempts=self._max_retries, base_delay=self._retry_base_delay
        )

    async def fetch_all(self, query: str, *args: Any) -> list[dict[str, Any]]:
        retryable = self._retryable_errors

        async def _op() -> list[dict[str, Any]]:
            async with self._pool.acquire() as conn:
                try:
                    async with conn.cursor() as cur:
                        await cur.execute(query, args or None)
                        return await cur.fetchall()  # type: ignore[return-value]
                except retryable as exc:
                    raise RetryableError(f"MySQL transient error: {exc}") from exc

        return await retry_async(
            _op, max_attempts=self._max_retries, base_delay=self._retry_base_delay
        )

    async def call_procedure(self, name: str, *args: Any) -> list[dict[str, Any]]:
        safe_name = validate_sql_identifier(name, kind="procedure")
        retryable = self._retryable_errors

        async def _op() -> list[dict[str, Any]]:
            async with self._pool.acquire() as conn:
                try:
                    async with conn.cursor() as cur:
                        await cur.callproc(safe_name, args)
                        await conn.commit()
                        return await cur.fetchall()  # type: ignore[return-value]
                except retryable as exc:
                    raise RetryableError(f"MySQL transient error: {exc}") from exc

        return await retry_async(
            _op, max_attempts=self._max_retries, base_delay=self._retry_base_delay
        )


# ─────────────────────────────────────────────────────────────────────────────
# PostgreSQL — asyncpg
# ─────────────────────────────────────────────────────────────────────────────


class PostgreSQLDatabase(Database):
    """Implementación para PostgreSQL usando asyncpg.

    Queries se escriben con %s (igual que MariaDB) — se convierten a $1, $2, ...
    internamente antes de enviarlas a asyncpg.
    """

    def __init__(self, dsn: str, **kwargs: Any) -> None:
        super().__init__(dsn, **kwargs)
        self._pool: Any = None
        self._retryable_errors: tuple[type[Exception], ...] = ()

    async def connect(self) -> None:
        import asyncpg

        self._retryable_errors = (
            asyncpg.TooManyConnectionsError,
            asyncpg.DeadlockDetectedError,
            asyncpg.CannotConnectNowError,
            asyncpg.ConnectionDoesNotExistError,
        )
        self._pool = await asyncpg.create_pool(
            self._dsn,
            min_size=self._min_size,
            max_size=self._max_size,
            command_timeout=self._command_timeout,
        )
        logger.info("postgres_pool_ready", min_size=self._min_size, max_size=self._max_size)

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    async def execute(self, query: str, *args: Any) -> str:
        pg_query = _to_positional(query)
        retryable = self._retryable_errors

        async def _op() -> str:
            async with self._pool.acquire() as conn:
                try:
                    status = await conn.execute(pg_query, *args)
                    return str(status)
                except retryable as exc:
                    raise RetryableError(f"PostgreSQL transient error: {exc}") from exc

        return await retry_async(
            _op, max_attempts=self._max_retries, base_delay=self._retry_base_delay
        )

    async def fetch_one(self, query: str, *args: Any) -> dict[str, Any] | None:
        pg_query = _to_positional(query)
        retryable = self._retryable_errors

        async def _op() -> dict[str, Any] | None:
            async with self._pool.acquire() as conn:
                try:
                    row = await conn.fetchrow(pg_query, *args)
                    return dict(row) if row is not None else None
                except retryable as exc:
                    raise RetryableError(f"PostgreSQL transient error: {exc}") from exc

        return await retry_async(
            _op, max_attempts=self._max_retries, base_delay=self._retry_base_delay
        )

    async def fetch_all(self, query: str, *args: Any) -> list[dict[str, Any]]:
        pg_query = _to_positional(query)
        retryable = self._retryable_errors

        async def _op() -> list[dict[str, Any]]:
            async with self._pool.acquire() as conn:
                try:
                    rows = await conn.fetch(pg_query, *args)
                    return [dict(row) for row in rows]
                except retryable as exc:
                    raise RetryableError(f"PostgreSQL transient error: {exc}") from exc

        return await retry_async(
            _op, max_attempts=self._max_retries, base_delay=self._retry_base_delay
        )

    async def call_procedure(self, name: str, *args: Any) -> list[dict[str, Any]]:
        """Llama una función que retorna filas via SELECT * FROM name($1, ...).

        En PostgreSQL las rutinas que devuelven conjuntos de filas son
        funciones (FUNCTION), no procedures (PROCEDURE). Por eso se usa
        SELECT * FROM en lugar de CALL.

        Si necesitás llamar un PROCEDURE sin resultado (OUT params),
        usá execute() directamente con 'CALL proc_name($1, ...)'.
        """
        safe_name = validate_sql_identifier(name, kind="procedure")
        placeholders = ", ".join(f"${i}" for i in range(1, len(args) + 1))
        query = f"SELECT * FROM {safe_name}({placeholders})"
        retryable = self._retryable_errors

        async def _op() -> list[dict[str, Any]]:
            async with self._pool.acquire() as conn:
                try:
                    rows = await conn.fetch(query, *args)
                    return [dict(row) for row in rows]
                except retryable as exc:
                    raise RetryableError(f"PostgreSQL transient error: {exc}") from exc

        return await retry_async(
            _op, max_attempts=self._max_retries, base_delay=self._retry_base_delay
        )


# ─────────────────────────────────────────────────────────────────────────────
# Factory — elige la implementación según el scheme del DSN
# ─────────────────────────────────────────────────────────────────────────────


def create_database(dsn: str, **kwargs: Any) -> Database:
    """Instancia el conector correcto según el scheme del DSN.

    Args:
        dsn: DSN de conexión.
             mysql://user:pass@host/db      → MariaDBDatabase
             postgresql://user:pass@host/db → PostgreSQLDatabase
        **kwargs: min_size, max_size, command_timeout, max_retries, retry_base_delay.

    Returns:
        Instancia de Database lista para llamar connect().

    Raises:
        ValueError: si el scheme no es soportado.
    """
    scheme = urlparse(dsn).scheme.lower().split("+")[0]
    if scheme in ("mysql", "mariadb"):
        return MariaDBDatabase(dsn, **kwargs)
    if scheme in ("postgresql", "postgres"):
        return PostgreSQLDatabase(dsn, **kwargs)
    raise ValueError(
        f"DSN scheme no soportado: {scheme!r}. Usar mysql:// o postgresql://"
    )
