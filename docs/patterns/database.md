# Pattern: Database — bulk insert e identifier validation

## Bulk insert multi-VALUES

Cuando un handler tiene que persistir N filas (ej. expandir un evento en
N destinatarios), evitar N inserts individuales:

```python
# ❌ N inserts = N roundtrips a la DB
for recipient in recipients:
    await db.execute("INSERT INTO ... VALUES ($1, $2)", recipient.id, recipient.phone)

# ✅ UN insert con múltiples VALUES
await db.insert_batch(
    table="recipients",
    columns=["id", "phone"],
    rows=[(r.id, r.phone) for r in recipients],
)
```

`Database.insert_batch()` construye una sola query del tipo:

```sql
INSERT INTO recipients (id, phone) VALUES ($1, $2), ($3, $4), ($5, $6), ...
```

Diferencia real: insertar 1000 filas pasa de ~500ms a ~50ms.

## Identifier validation

`asyncpg` solo soporta placeholders para **valores**, no para nombres de
tablas o stored procedures. Cuando el nombre viene de input/config, NO
podés interpolarlo seguramente.

`src/core/utils.py:validate_sql_identifier()` rechaza cualquier nombre que
no matchee `^[a-zA-Z_][a-zA-Z0-9_]*$`:

```python
from src.core.utils import validate_sql_identifier

# Antes de pasar a una query construida con f-string
safe_table = validate_sql_identifier(user_provided_table, kind="table")
query = f"DELETE FROM {safe_table} WHERE id = $1"
await db.execute(query, item_id)
```

`Database.call_procedure()` y `Database.insert_batch()` ya validan
internamente. Solo necesitás llamar `validate_sql_identifier` directo si
construís SQL custom con identificadores dinámicos.

## Reglas duras

- **Valores**: SIEMPRE como parámetros (`$1`, `$2`, ...). NUNCA en f-string.
- **Identificadores dinámicos**: SIEMPRE pasarlos por `validate_sql_identifier`.
- **Identificadores estáticos** (hardcoded en el código): no necesitan validación.

## Retry con jitter

`Database.execute/fetch_one/fetch_all` reintentan automáticamente errores
transient de Postgres (deadlock, serialization, connection lost). El
backoff usa jitter para evitar thundering herd. Los errores permanentes
(syntax error, constraint violation, etc) NO se reintentan.

Lista de errores retryables está en `src/db/database.py:_RETRYABLE_PG_ERRORS`.
