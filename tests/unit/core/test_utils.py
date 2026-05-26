"""Tests de validate_sql_identifier."""

from __future__ import annotations

import pytest

from src.core.utils import validate_sql_identifier


class TestValidateSqlIdentifier:
    @pytest.mark.parametrize(
        "name",
        ["users", "User_Profile", "_private", "tbl123", "a", "A_B_C"],
    )
    def test_accepts_valid(self, name: str) -> None:
        assert validate_sql_identifier(name) == name

    @pytest.mark.parametrize(
        "name",
        [
            "1users",        # empieza con dígito
            "user-profile",  # guion
            "user.profile",  # punto
            "user profile",  # espacio
            "users;",        # punto y coma
            "users--",       # comentario SQL
            "DROP TABLE",    # SQL injection naive
            "'; DROP--",     # SQL injection clásico
            "",              # vacío
            "user'name",     # comilla
        ],
    )
    def test_rejects_invalid(self, name: str) -> None:
        with pytest.raises(ValueError, match="Invalid SQL"):
            validate_sql_identifier(name)

    def test_kind_appears_in_error(self) -> None:
        with pytest.raises(ValueError, match="Invalid SQL table"):
            validate_sql_identifier("bad-name", kind="table")
