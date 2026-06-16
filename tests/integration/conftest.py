"""Fixtures para tests de integración — Testcontainers con Redpanda, Redis, Postgres, MariaDB.

Los contenedores se levantan UNA VEZ por sesión (scope="session") para no
pagar el costo de bootstrap en cada test. El aislamiento entre tests viene
de:
- topics únicos por test (random suffix)
- group_ids únicos por test
- clean-up de Redis namespaces
- tablas aisladas por test en DB (creación/drop en cada fixture)

Marcar tests con `@pytest.mark.integration` para correrlos selectivamente:
    uv run pytest tests/integration/ -v
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator, Iterator

import pytest
import pytest_asyncio
from redis.asyncio import Redis, from_url
from testcontainers.kafka import RedpandaContainer
from testcontainers.mysql import MySqlContainer
from testcontainers.postgres import PostgresContainer
from testcontainers.redis import RedisContainer


@pytest.fixture(scope="session")
def redpanda() -> Iterator[RedpandaContainer]:
    with RedpandaContainer("redpandadata/redpanda:latest") as container:
        yield container


@pytest.fixture(scope="session")
def postgres() -> Iterator[PostgresContainer]:
    with PostgresContainer("postgres:16-alpine") as container:
        yield container


@pytest.fixture(scope="session")
def mariadb() -> Iterator[MySqlContainer]:
    with MySqlContainer("mariadb:11", username="test", password="test", dbname="test_consumer") as container:
        yield container


@pytest.fixture(scope="session")
def redis_container() -> Iterator[RedisContainer]:
    with RedisContainer("redis:7-alpine") as container:
        yield container


@pytest.fixture(scope="session")
def kafka_bootstrap(redpanda: RedpandaContainer) -> str:
    return redpanda.get_bootstrap_server()


@pytest.fixture(scope="session")
def postgres_dsn(postgres: PostgresContainer) -> str:
    return (
        f"postgresql://{postgres.username}:{postgres.password}"
        f"@{postgres.get_container_host_ip()}:{postgres.get_exposed_port(5432)}"
        f"/{postgres.dbname}"
    )


@pytest.fixture(scope="session")
def mariadb_dsn(mariadb: MySqlContainer) -> str:
    return (
        f"mysql://{mariadb.username}:{mariadb.password}"
        f"@{mariadb.get_container_host_ip()}:{mariadb.get_exposed_port(3306)}"
        f"/{mariadb.dbname}"
    )


@pytest.fixture(scope="session")
def redis_url(redis_container: RedisContainer) -> str:
    host = redis_container.get_container_host_ip()
    port = redis_container.get_exposed_port(6379)
    return f"redis://{host}:{port}/0"


@pytest_asyncio.fixture
async def redis_client(redis_url: str) -> AsyncIterator[Redis]:
    """Cliente Redis async — flush al terminar para aislamiento entre tests."""
    client: Redis = from_url(redis_url, decode_responses=True)
    yield client
    await client.flushdb()
    await client.aclose()


@pytest.fixture
def unique_topic() -> str:
    """Topic único por test — evita bleed-through entre tests paralelos."""
    return f"test-events-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def unique_group_id() -> str:
    return f"test-group-{uuid.uuid4().hex[:8]}"
