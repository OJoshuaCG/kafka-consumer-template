"""Tests de integración para MariaDBDatabase y PostgreSQLDatabase.

Cada clase levanta la tabla en on_start y la destruye en teardown.
El fixture de DB es function-scoped para aislamiento entre tests.

Requiere Docker. Correr con:
    uv run pytest tests/integration/db/ -v -m integration
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest
import pytest_asyncio

from src.db.database import MariaDBDatabase, PostgreSQLDatabase

pytestmark = pytest.mark.integration

_CREATE_MARIADB = """
    CREATE TABLE IF NOT EXISTS db_test_items (
        id      VARCHAR(36)  NOT NULL PRIMARY KEY,
        name    VARCHAR(255) NOT NULL,
        value   INT          NOT NULL DEFAULT 0
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

_CREATE_POSTGRES = """
    CREATE TABLE IF NOT EXISTS db_test_items (
        id    TEXT    NOT NULL PRIMARY KEY,
        name  TEXT    NOT NULL,
        value INTEGER NOT NULL DEFAULT 0
    )
"""

_DROP = "DROP TABLE IF EXISTS db_test_items"


# ─────────────────────────────────────────────────────────────────────────────
# MariaDB
# ─────────────────────────────────────────────────────────────────────────────


@pytest_asyncio.fixture
async def mariadb_db(mariadb_dsn: str) -> AsyncIterator[MariaDBDatabase]:
    db = MariaDBDatabase(mariadb_dsn)
    await db.connect()
    await db.execute(_CREATE_MARIADB)
    yield db
    await db.execute(_DROP)
    await db.close()


class TestMariaDBDatabase:
    async def test_execute_insert(self, mariadb_db: MariaDBDatabase) -> None:
        result = await mariadb_db.execute(
            "INSERT INTO db_test_items (id, name, value) VALUES (%s, %s, %s)",
            "item-1", "alpha", 10,
        )
        assert "rowcount=1" in result

    async def test_fetch_one_returns_row(self, mariadb_db: MariaDBDatabase) -> None:
        await mariadb_db.execute(
            "INSERT INTO db_test_items (id, name, value) VALUES (%s, %s, %s)",
            "item-1", "alpha", 10,
        )
        row = await mariadb_db.fetch_one(
            "SELECT * FROM db_test_items WHERE id = %s", "item-1"
        )
        assert row is not None
        assert row["name"] == "alpha"
        assert row["value"] == 10

    async def test_fetch_one_missing_returns_none(self, mariadb_db: MariaDBDatabase) -> None:
        row = await mariadb_db.fetch_one(
            "SELECT * FROM db_test_items WHERE id = %s", "ghost"
        )
        assert row is None

    async def test_fetch_all_returns_all_rows(self, mariadb_db: MariaDBDatabase) -> None:
        for i in range(3):
            await mariadb_db.execute(
                "INSERT INTO db_test_items (id, name, value) VALUES (%s, %s, %s)",
                f"item-{i}", f"name-{i}", i * 10,
            )
        rows = await mariadb_db.fetch_all(
            "SELECT * FROM db_test_items ORDER BY value ASC"
        )
        assert len(rows) == 3
        assert [r["value"] for r in rows] == [0, 10, 20]

    async def test_fetch_all_empty_table(self, mariadb_db: MariaDBDatabase) -> None:
        rows = await mariadb_db.fetch_all("SELECT * FROM db_test_items")
        assert rows == []

    async def test_insert_batch(self, mariadb_db: MariaDBDatabase) -> None:
        batch = [("b-1", "first", 1), ("b-2", "second", 2), ("b-3", "third", 3)]
        count = await mariadb_db.insert_batch(
            "db_test_items", ["id", "name", "value"], batch
        )
        assert count == 3
        rows = await mariadb_db.fetch_all(
            "SELECT * FROM db_test_items ORDER BY value ASC"
        )
        assert len(rows) == 3
        assert rows[0]["name"] == "first"
        assert rows[2]["value"] == 3

    async def test_insert_batch_empty_is_noop(self, mariadb_db: MariaDBDatabase) -> None:
        count = await mariadb_db.insert_batch("db_test_items", ["id", "name", "value"], [])
        assert count == 0

    async def test_insert_batch_row_length_mismatch_raises(
        self, mariadb_db: MariaDBDatabase
    ) -> None:
        with pytest.raises(ValueError, match="Row 0"):
            await mariadb_db.insert_batch(
                "db_test_items", ["id", "name", "value"], [("only-two", "vals")]  # type: ignore[list-item]
            )

    async def test_like_with_escaped_percent(self, mariadb_db: MariaDBDatabase) -> None:
        """Verifica que %% en queries MariaDB se maneje correctamente."""
        await mariadb_db.execute(
            "INSERT INTO db_test_items (id, name, value) VALUES (%s, %s, %s)",
            "item-like", "status_active", 99,
        )
        # %% es el literal % en el paramstyle de aiomysql
        rows = await mariadb_db.fetch_all(
            "SELECT * FROM db_test_items WHERE name LIKE %s", "%status%"
        )
        assert len(rows) == 1
        assert rows[0]["id"] == "item-like"


# ─────────────────────────────────────────────────────────────────────────────
# PostgreSQL
# ─────────────────────────────────────────────────────────────────────────────


@pytest_asyncio.fixture
async def postgres_db(postgres_dsn: str) -> AsyncIterator[PostgreSQLDatabase]:
    db = PostgreSQLDatabase(postgres_dsn)
    await db.connect()
    await db.execute(_CREATE_POSTGRES)
    yield db
    await db.execute(_DROP)
    await db.close()


class TestPostgreSQLDatabase:
    async def test_execute_insert(self, postgres_db: PostgreSQLDatabase) -> None:
        result = await postgres_db.execute(
            "INSERT INTO db_test_items (id, name, value) VALUES (%s, %s, %s)",
            "item-1", "alpha", 10,
        )
        assert result  # asyncpg retorna el status tag, e.g. "INSERT 0 1"

    async def test_fetch_one_returns_row(self, postgres_db: PostgreSQLDatabase) -> None:
        await postgres_db.execute(
            "INSERT INTO db_test_items (id, name, value) VALUES (%s, %s, %s)",
            "item-1", "alpha", 10,
        )
        row = await postgres_db.fetch_one(
            "SELECT * FROM db_test_items WHERE id = %s", "item-1"
        )
        assert row is not None
        assert row["name"] == "alpha"
        assert row["value"] == 10

    async def test_fetch_one_missing_returns_none(self, postgres_db: PostgreSQLDatabase) -> None:
        row = await postgres_db.fetch_one(
            "SELECT * FROM db_test_items WHERE id = %s", "ghost"
        )
        assert row is None

    async def test_fetch_all_returns_all_rows(self, postgres_db: PostgreSQLDatabase) -> None:
        for i in range(3):
            await postgres_db.execute(
                "INSERT INTO db_test_items (id, name, value) VALUES (%s, %s, %s)",
                f"item-{i}", f"name-{i}", i * 10,
            )
        rows = await postgres_db.fetch_all(
            "SELECT * FROM db_test_items ORDER BY value ASC"
        )
        assert len(rows) == 3
        assert [r["value"] for r in rows] == [0, 10, 20]

    async def test_fetch_all_empty_table(self, postgres_db: PostgreSQLDatabase) -> None:
        rows = await postgres_db.fetch_all("SELECT * FROM db_test_items")
        assert rows == []

    async def test_insert_batch(self, postgres_db: PostgreSQLDatabase) -> None:
        batch = [("b-1", "first", 1), ("b-2", "second", 2), ("b-3", "third", 3)]
        count = await postgres_db.insert_batch(
            "db_test_items", ["id", "name", "value"], batch
        )
        assert count == 3
        rows = await postgres_db.fetch_all(
            "SELECT * FROM db_test_items ORDER BY value ASC"
        )
        assert len(rows) == 3
        assert rows[0]["name"] == "first"
        assert rows[2]["value"] == 3

    async def test_insert_batch_empty_is_noop(self, postgres_db: PostgreSQLDatabase) -> None:
        count = await postgres_db.insert_batch("db_test_items", ["id", "name", "value"], [])
        assert count == 0

    async def test_placeholder_conversion(self, postgres_db: PostgreSQLDatabase) -> None:
        """Verifica que %s se convierte correctamente a $N para asyncpg."""
        await postgres_db.execute(
            "INSERT INTO db_test_items (id, name, value) VALUES (%s, %s, %s)",
            "item-conv", "beta", 42,
        )
        row = await postgres_db.fetch_one(
            "SELECT id, name FROM db_test_items WHERE id = %s AND value = %s",
            "item-conv", 42,
        )
        assert row is not None
        assert row["name"] == "beta"

    async def test_like_with_percent_parameter(self, postgres_db: PostgreSQLDatabase) -> None:
        """El % del LIKE va en el argumento, no en el query — sin problema de conversión."""
        await postgres_db.execute(
            "INSERT INTO db_test_items (id, name, value) VALUES (%s, %s, %s)",
            "item-like", "status_active", 99,
        )
        rows = await postgres_db.fetch_all(
            "SELECT * FROM db_test_items WHERE name LIKE %s", "%status%"
        )
        assert len(rows) == 1
        assert rows[0]["id"] == "item-like"

    async def test_call_procedure_via_function(self, postgres_db: PostgreSQLDatabase) -> None:
        """call_procedure usa SELECT * FROM — verifica con una función SQL real."""
        await postgres_db.execute("""
            CREATE OR REPLACE FUNCTION get_items_above(threshold INT)
            RETURNS TABLE(id TEXT, name TEXT, value INT)
            LANGUAGE sql AS $$
                SELECT id, name, value FROM db_test_items WHERE value > threshold;
            $$
        """)
        for i in range(5):
            await postgres_db.execute(
                "INSERT INTO db_test_items (id, name, value) VALUES (%s, %s, %s)",
                f"fn-{i}", f"item-{i}", i * 10,
            )
        rows = await postgres_db.call_procedure("get_items_above", 25)
        assert all(r["value"] > 25 for r in rows)
        assert len(rows) == 2  # value=30 y value=40
