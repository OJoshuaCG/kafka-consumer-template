"""Utilidades compartidas del core."""

from __future__ import annotations

import re

_SQL_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def validate_sql_identifier(name: str, *, kind: str = "identifier") -> str:
    """Valida que `name` sea un identificador SQL seguro.

    Para casos donde un identificador (nombre de tabla, stored procedure) viene
    de configuración o input y NO puede pasarse como parámetro. La regex
    `^[a-zA-Z_][a-zA-Z0-9_]*$` es deliberadamente conservadora — rechaza
    espacios, comillas, puntos, y cualquier metacarácter SQL.

    Args:
        name: el identificador a validar.
        kind: descripción para el mensaje de error (ej. "table name", "procedure").

    Returns:
        El mismo `name` si es válido.

    Raises:
        ValueError: si el identificador no matchea la regex.
    """
    if not _SQL_IDENTIFIER_RE.match(name):
        raise ValueError(f"Invalid SQL {kind}: {name!r}. Must match {_SQL_IDENTIFIER_RE.pattern}")
    return name
