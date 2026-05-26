# Producer Demo

**Dev tool** — disparar eventos hacia Redpanda local desde una UI Swagger.

> ⚠️ NUNCA va a producción. Esto vive en `[dependency-groups] dev` del `pyproject.toml`,
> no se empaqueta en el wheel, no se copia al Dockerfile runtime stage, y `ruff` rechaza
> `import fastapi` desde `src/`.

## Uso

```bash
# Levantar Redpanda + Redis + Postgres
docker compose up -d

# Iniciar el producer demo
uv run uvicorn tools.producer_demo.main:app --reload

# Abrir Swagger
open http://localhost:8000/docs
```

Desde la UI, `POST /publish/example` con un body como:

```json
{
  "type": "greeting",
  "user_id": "user-123",
  "message": "Hola mundo"
}
```

Y mirar los logs del consumer (`uv run example-consumer`) — debería ver el evento
procesado con `message_id`, `consumer_name`, `topic` propagados vía ContextVars.

## Para qué NO usarlo

- Tests automatizados — usar `tests/unit/` (pytest puro) o `tests/integration/` (Testcontainers).
- Producción — usar el producer real de tu sistema upstream.
- Performance testing — usar `kafka-producer-perf-test` o similar.
