# syntax=docker/dockerfile:1.7
# =============================================================================
# Multi-stage build con layer caching agresivo y FastAPI fuera de la imagen.
#
# Layer caching: COPY de pyproject.toml + uv.lock ANTES de COPY de src/.
# Cambios en código no invalidan la layer de deps → rebuilds en segundos.
#
# Dependency separation: `uv sync --no-dev --no-install-project` excluye
# explícitamente [dependency-groups] dev (FastAPI/uvicorn) y test (pytest/etc).
# La imagen final NO tiene FastAPI ni `tools/`.
# =============================================================================

ARG PYTHON_VERSION=3.13
ARG UV_VERSION=0.5.13

# -----------------------------------------------------------------------------
# Stage 1: builder — instala dependencias en una capa cacheable
# -----------------------------------------------------------------------------
FROM python:${PYTHON_VERSION}-slim-bookworm AS builder

COPY --from=ghcr.io/astral-sh/uv:0.5.13 /uv /uvx /bin/

ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_DOWNLOADS=never \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Capa 1: SOLO deps. Esta capa se reusa entre builds si pyproject.toml/uv.lock
# no cambiaron. `--no-dev` excluye dev/test/lint groups → FastAPI fuera.
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project

# Capa 2: código del proyecto. Cambios acá NO invalidan la capa de deps.
COPY src/ ./src/
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev

# -----------------------------------------------------------------------------
# Stage 2: runtime — minimal, non-root, sin tools/, sin tests/
# -----------------------------------------------------------------------------
FROM python:${PYTHON_VERSION}-slim-bookworm AS runtime

# Usuario non-root (uid 1000)
RUN groupadd --system --gid 1000 app \
 && useradd --system --uid 1000 --gid app --shell /sbin/nologin --create-home app

WORKDIR /app

# Copiar SOLO el venv y src/ desde el builder.
# tools/ NO se copia. tests/ NO se copia. k8s/ NO se copia.
COPY --from=builder --chown=app:app /app/.venv /app/.venv
COPY --from=builder --chown=app:app /app/src /app/src

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONPATH="/app" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

USER app

# Default: example-consumer. Override con `docker run ... <otro-consumer>`.
CMD ["python", "-m", "src.consumers.example.consumer"]

# Healthcheck para `docker ps` — K8s usa exec probe propio en deployment.yaml
HEALTHCHECK --interval=15s --timeout=5s --start-period=20s --retries=3 \
    CMD python -c "import time, os; assert time.time() - os.path.getmtime('/tmp/healthcheck') < 60"
