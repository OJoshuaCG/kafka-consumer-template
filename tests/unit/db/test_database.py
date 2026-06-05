"""Tests unitarios para la capa de DB — sin infra real.

Cubre:
- _to_positional: conversión de placeholders
- create_database: factory devuelve la subclase correcta
"""

from __future__ import annotations

import pytest

from src.db.database import MariaDBDatabase, PostgreSQLDatabase, _to_positional, create_database


class TestToPositional:
    def test_single_placeholder(self) -> None:
        assert _to_positional("SELECT %s") == "SELECT $1"

    def test_multiple_placeholders(self) -> None:
        assert _to_positional("INSERT INTO t VALUES (%s, %s, %s)") == "INSERT INTO t VALUES ($1, $2, $3)"

    def test_no_placeholders(self) -> None:
        assert _to_positional("SELECT 1") == "SELECT 1"

    def test_escaped_percent_literal(self) -> None:
        assert _to_positional("WHERE name LIKE '%%status%%'") == "WHERE name LIKE '%status%'"

    def test_mixed_placeholder_and_escaped_percent(self) -> None:
        result = _to_positional("WHERE name LIKE '%%' AND id = %s")
        assert result == "WHERE name LIKE '%' AND id = $1"

    def test_other_percent_sequences_unchanged(self) -> None:
        assert _to_positional("FORMAT '%.2f'") == "FORMAT '%.2f'"

    def test_sequential_numbering(self) -> None:
        result = _to_positional("%s AND %s AND %s")
        assert result == "$1 AND $2 AND $3"


class TestCreateDatabase:
    def test_mysql_scheme_returns_mariadb(self) -> None:
        db = create_database("mysql://user:pass@localhost/mydb")
        assert isinstance(db, MariaDBDatabase)

    def test_mariadb_scheme_returns_mariadb(self) -> None:
        db = create_database("mariadb://user:pass@localhost/mydb")
        assert isinstance(db, MariaDBDatabase)

    def test_postgresql_scheme_returns_postgres(self) -> None:
        db = create_database("postgresql://user:pass@localhost/mydb")
        assert isinstance(db, PostgreSQLDatabase)

    def test_postgres_scheme_returns_postgres(self) -> None:
        db = create_database("postgres://user:pass@localhost/mydb")
        assert isinstance(db, PostgreSQLDatabase)

    def test_mysql_plus_driver_scheme(self) -> None:
        db = create_database("mysql+aiomysql://user:pass@localhost/mydb")
        assert isinstance(db, MariaDBDatabase)

    def test_postgres_plus_driver_scheme(self) -> None:
        db = create_database("postgresql+asyncpg://user:pass@localhost/mydb")
        assert isinstance(db, PostgreSQLDatabase)

    def test_unsupported_scheme_raises(self) -> None:
        with pytest.raises(ValueError, match="no soportado"):
            create_database("sqlite:///mydb.sqlite")

    def test_kwargs_forwarded(self) -> None:
        db = create_database("mysql://user:pass@localhost/mydb", max_size=20, max_retries=5)
        assert isinstance(db, MariaDBDatabase)
        assert db._max_size == 20
        assert db._max_retries == 5
