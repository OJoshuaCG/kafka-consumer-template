# Configuración

## Sistema de settings

El proyecto usa **Pydantic Settings** (`pydantic-settings`) para gestionar
variables de entorno. Hay dos niveles:

1. **`GlobalSettings`** (`src/config/settings.py`): variables compartidas
   por todo el proyecto — Kafka, Redis, Postgres, logging, métricas.
2. **Settings por consumer** (`src/consumers/<name>/settings.py`): variables
   específicas del consumer — topic, group_id, DLQ, etc.

Cada level tiene su propio `env_prefix` para evitar colisiones cuando múltiples
consumers corren en el mismo proceso o leen el mismo `.env`.

---

## GlobalSettings — variables compartidas

Prefijo: ninguno (vacío).

| Variable | Default | Descripción |
|---|---|---|
| `ENVIRONMENT` | `development` | `development`, `staging`, `production` |
| `LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL` |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Brokers Kafka/Redpanda separados por coma |
| `KAFKA_SECURITY_PROTOCOL` | `PLAINTEXT` | `PLAINTEXT`, `SASL_SSL`, `SSL` |
| `KAFKA_SASL_USERNAME` | `None` | Usuario SASL (solo si `SASL_SSL`) |
| `KAFKA_SASL_PASSWORD` | `None` | Contraseña SASL (solo si `SASL_SSL`) |
| `DATABASE_URL` | `postgresql://kafka:kafka@localhost:5432/kafka_consumer` | DSN asyncpg |
| `REDIS_URL` | `redis://localhost:6379/0` | URL Redis |
| `METRICS_PORT` | `9090` | Puerto HTTP para `/metrics` |
| `METRICS_ENABLED` | `True` | Habilitar/deshabilitar exposición de métricas |
| `HEALTH_FILE_PATH` | `/tmp/healthcheck` | Path del archivo de healthcheck |
| `HEALTH_WRITE_INTERVAL_SECONDS` | `10.0` | Frecuencia de escritura del healthcheck |

---

## Settings por consumer — ejemplo

Prefijo: `EXAMPLE_` (cada consumer define el suyo).

| Variable | Default | Descripción |
|---|---|---|
| `EXAMPLE_TOPIC` | `example-events` | Topic a consumir |
| `EXAMPLE_GROUP_ID` | `example-consumer` | Consumer group ID |
| `EXAMPLE_DLQ_TOPIC` | `example-events-dlq` | Topic DLQ |

Para un consumer de WhatsApp el prefijo sería `WHATSAPP_`, para pagos `PAYMENTS_`, etc.
Esto permite tener todos los consumers en un solo `.env` sin colisiones.

---

## Cómo agregar settings a un consumer nuevo

```python
# src/consumers/mi_consumer/settings.py
from functools import lru_cache
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class MiConsumerSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="MICONSUMER_",    # ← ÚNICO por consumer
        extra="ignore",
    )

    topic: str = Field("mi-consumer-events")
    group_id: str = Field("mi-consumer")
    dlq_topic: str = Field("mi-consumer-events-dlq")

    # Settings específicos del consumer:
    batch_size: int = Field(100, description="Tamaño máximo de batch para bulk insert")
    upstream_api_url: str = Field("https://api.ejemplo.com")
    upstream_timeout_seconds: float = Field(10.0)


@lru_cache
def get_mi_consumer_settings() -> MiConsumerSettings:
    return MiConsumerSettings()
```

El `@lru_cache` garantiza una sola instancia por proceso. En tests, llamar
`get_mi_consumer_settings.cache_clear()` para forzar recarga.

---

## Archivo .env

Usar `.env.example` como punto de partida:

```bash
cp .env.example .env
```

El `.env.example` documenta todas las variables disponibles con sus defaults y
comentarios. El `.env` real nunca debe commitearse (ya está en `.gitignore`).

Ejemplo de `.env` para desarrollo local:

```bash
# Global
ENVIRONMENT=development
LOG_LEVEL=DEBUG

# Kafka / Redpanda
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_SECURITY_PROTOCOL=PLAINTEXT

# Database
DATABASE_URL=postgresql://kafka:kafka@localhost:5432/kafka_consumer

# Redis
REDIS_URL=redis://localhost:6379/0

# Métricas
METRICS_PORT=9090
METRICS_ENABLED=true

# Example consumer
EXAMPLE_TOPIC=example-events
EXAMPLE_GROUP_ID=example-consumer
EXAMPLE_DLQ_TOPIC=example-events-dlq
```

---

## Producción — K8s secrets

En K8s las credenciales **no van en ConfigMaps**. Van en Secrets referenciados
como env vars en el Deployment. Ver `k8s/deployment.yaml`:

```yaml
env:
  - name: KAFKA_SASL_USERNAME
    valueFrom:
      secretKeyRef:
        name: kafka-credentials
        key: username
  - name: KAFKA_SASL_PASSWORD
    valueFrom:
      secretKeyRef:
        name: kafka-credentials
        key: password
  - name: DATABASE_URL
    valueFrom:
      secretKeyRef:
        name: db-credentials
        key: url
```

Los secrets se crean con:

```bash
kubectl create secret generic kafka-credentials \
  --from-literal=username=mi_usuario \
  --from-literal=password=mi_contraseña

kubectl create secret generic db-credentials \
  --from-literal=url="postgresql://user:pass@host:5432/db"
```

---

## Producción — Kafka con SASL_SSL

```bash
KAFKA_SECURITY_PROTOCOL=SASL_SSL
KAFKA_SASL_USERNAME=mi_usuario_redpanda
KAFKA_SASL_PASSWORD=mi_contraseña_redpanda
KAFKA_BOOTSTRAP_SERVERS=broker1.redpanda.cloud:9092,broker2.redpanda.cloud:9092
```

`KafkaClientFactory` en `src/core/client.py` detecta automáticamente si
`KAFKA_SECURITY_PROTOCOL` es `SASL_SSL` y configura el mecanismo SCRAM-SHA-256.

---

## Acceder a settings en código

```python
# Settings globales
from src.config.settings import get_global_settings
settings = get_global_settings()
print(settings.kafka_bootstrap_servers)

# Settings de un consumer específico
from src.consumers.example.settings import get_example_settings
s = get_example_settings()
print(s.topic, s.group_id)
```

En tests, limpiar el caché para que los tests puedan inyectar variables distintas:

```python
import pytest
from src.config.settings import get_global_settings

@pytest.fixture(autouse=True)
def clear_settings_cache():
    get_global_settings.cache_clear()
    yield
    get_global_settings.cache_clear()
```
