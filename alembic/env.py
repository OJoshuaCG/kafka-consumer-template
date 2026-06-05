"""Alembic env — integrado con Pydantic Settings.

Lee la `DATABASE_URL` de las settings globales en vez de alembic.ini, así
no duplicamos config. Para que Alembic vea sus migrations:

    uv run alembic revision --autogenerate -m "add greetings table"
    uv run alembic upgrade head
"""

from __future__ import annotations

from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

from src.config.settings import get_global_settings

# Alembic Config — sigue funcionando para logging, etc.
config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Override de la URL desde settings.
# aiomysql es async — Alembic necesita el driver sync pymysql.
settings = get_global_settings()
_url = settings.database_url
sync_url = (
    _url.replace("mysql+aiomysql://", "mysql+pymysql://", 1)
    if "mysql+aiomysql://" in _url
    else _url.replace("mysql://", "mysql+pymysql://", 1).replace("mariadb://", "mysql+pymysql://", 1)
)
config.set_main_option("sqlalchemy.url", sync_url)

# Para autogenerate. Si en el futuro agregamos modelos SQLAlchemy:
#   from src.db.models import Base
#   target_metadata = Base.metadata
target_metadata = None


def run_migrations_offline() -> None:
    """Migrations en modo 'offline' — emite SQL sin engine."""
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Migrations en modo 'online' — con un engine real."""
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
