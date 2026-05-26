# Desplegar en infraestructura existente

Esta guía asume que ya tenés corriendo:

- ✅ Un cluster Kafka o Redpanda (broker URL disponible)
- ✅ Redis (opcional pero recomendado para idempotencia)
- ✅ PostgreSQL (solo si el consumer persiste datos)

El `docker-compose.yml` del proyecto es **solo para desarrollo local** — no se usa aquí.

---

## 1. Variables de entorno mínimas

Copiá `.env.example` y editá los valores:

```bash
cp .env.example .env
```

Las variables globales que siempre necesitás:

```bash
# Infra
KAFKA_BOOTSTRAP_SERVERS=broker1:9092,broker2:9092
REDIS_URL=redis://tu-redis:6379/0
DATABASE_URL=postgresql://user:pass@tu-postgres:5432/mi_db   # solo si usás DB

# Runtime
ENVIRONMENT=production
LOG_LEVEL=INFO
METRICS_PORT=9090

# TLS / SASL (si el broker requiere autenticación)
KAFKA_SECURITY_PROTOCOL=SASL_SSL
KAFKA_SASL_USERNAME=mi_usuario
KAFKA_SASL_PASSWORD=mi_password
```

Más las variables específicas de tu consumer (ver `src/consumers/<nombre>/settings.py`).
Por ejemplo, para el example consumer:

```bash
EXAMPLE_TOPIC=mi-topic-de-eventos
EXAMPLE_GROUP_ID=mi-consumer-group
EXAMPLE_DLQ_TOPIC=mi-topic-dlq
```

El prefijo (`EXAMPLE_`, `WHATSAPP_`, etc.) viene del `env_prefix` de cada consumer.
Un solo `.env` puede tener los env vars de múltiples consumers sin colisión.

---

## 2. Build de la imagen

```bash
# Build estándar
docker build -t mi-consumer:latest .

# Con tag semver o git SHA (recomendado para producción)
docker build -t mi-consumer:1.0.0 .
docker build -t mi-consumer:$(git rev-parse --short HEAD) .

# Verificar que FastAPI NO está en la imagen de producción
docker run --rm mi-consumer:latest python -c "import fastapi" \
  && echo "FAIL: fastapi presente" \
  || echo "OK: fastapi ausente"
```

El Dockerfile es multi-stage: solo `src/` llega a la imagen final. `tools/`,
`tests/`, `k8s/` quedan fuera por construcción.

---

## 3. Correr con Docker

```bash
# Con archivo .env
docker run --rm --env-file .env mi-consumer:latest

# Con env vars inline
docker run --rm \
  -e KAFKA_BOOTSTRAP_SERVERS=broker1:9092 \
  -e REDIS_URL=redis://mi-redis:6379/0 \
  -e DATABASE_URL=postgresql://user:pass@mi-postgres/db \
  -e EXAMPLE_TOPIC=mi-topic \
  -e EXAMPLE_GROUP_ID=mi-group \
  -e ENVIRONMENT=production \
  mi-consumer:latest

# Si la imagen necesita acceder al host (ej: broker en localhost del host)
docker run --rm --env-file .env \
  --add-host=host.docker.internal:host-gateway \
  -e KAFKA_BOOTSTRAP_SERVERS=host.docker.internal:9092 \
  mi-consumer:latest
```

El comando por default en la imagen corre el `example-consumer`. Para un consumer
propio, overrideá el CMD:

```bash
docker run --rm --env-file .env mi-consumer:latest \
  python -m src.consumers.mi_consumer.consumer
```

---

## 4. Desplegar en K3s / Kubernetes

### 4.1 Subir la imagen a un registry accesible

```bash
# Docker Hub
docker tag mi-consumer:1.0.0 mi-usuario/mi-consumer:1.0.0
docker push mi-usuario/mi-consumer:1.0.0

# Registry privado (ej: Harbor, ECR, GCR)
docker tag mi-consumer:1.0.0 registry.mi-empresa.com/backend/mi-consumer:1.0.0
docker push registry.mi-empresa.com/backend/mi-consumer:1.0.0
```

Si usás k3s con registry local (airgapped), podés importar la imagen directamente:

```bash
# Exportar imagen
docker save mi-consumer:1.0.0 | gzip > mi-consumer.tar.gz

# Importar en k3s (en el nodo)
sudo k3s ctr images import mi-consumer.tar.gz
```

### 4.2 Crear los Secrets

```bash
# Kafka credentials (si usás SASL)
kubectl create secret generic kafka-credentials \
  -n mi-namespace \
  --from-literal=username=mi_usuario \
  --from-literal=password=mi_password

# Database
kubectl create secret generic db-credentials \
  -n mi-namespace \
  --from-literal=url="postgresql://user:pass@host:5432/db"

# Redis
kubectl create secret generic redis-credentials \
  -n mi-namespace \
  --from-literal=url="redis://:password@host:6379/0"
```

### 4.3 Adaptar el Deployment

Copiá `k8s/deployment.yaml` y editá los campos marcados:

```yaml
# k8s/mi-consumer-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: mi-consumer          # <-- cambiar
  namespace: mi-namespace    # <-- cambiar
spec:
  replicas: 2
  selector:
    matchLabels:
      app: mi-consumer       # <-- cambiar (mismo que metadata.name)
  template:
    metadata:
      labels:
        app: mi-consumer     # <-- cambiar
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "9090"
    spec:
      containers:
        - name: mi-consumer  # <-- cambiar
          image: registry.mi-empresa.com/backend/mi-consumer:1.0.0  # <-- cambiar
          command:
            - python
            - -m
            - src.consumers.mi_consumer.consumer  # <-- cambiar al entry point real

          env:
            # Configuración del consumer (no sensible)
            - name: KAFKA_BOOTSTRAP_SERVERS
              value: "broker1.mi-empresa.com:9092,broker2.mi-empresa.com:9092"
            - name: ENVIRONMENT
              value: production
            - name: LOG_LEVEL
              value: INFO
            - name: KAFKA_SECURITY_PROTOCOL
              value: SASL_SSL
            - name: MICONS_TOPIC      # prefijo del consumer
              value: mi-topic-prod
            - name: MICONS_GROUP_ID
              value: mi-consumer-group-prod

            # Credenciales desde Secrets
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
            - name: REDIS_URL
              valueFrom:
                secretKeyRef:
                  name: redis-credentials
                  key: url

          resources:
            requests:
              memory: "128Mi"
              cpu: "100m"
            limits:
              memory: "512Mi"
              cpu: "500m"

          # Health check — el consumer escribe /tmp/healthcheck cada 10s
          livenessProbe:
            exec:
              command:
                - python
                - -c
                - |
                  import os, sys, time
                  path = os.environ.get("HEALTH_FILE_PATH", "/tmp/healthcheck")
                  if not os.path.exists(path):
                      sys.exit(1)
                  sys.exit(0 if time.time() - os.path.getmtime(path) < 60 else 1)
            initialDelaySeconds: 15
            periodSeconds: 20
            failureThreshold: 3

          volumeMounts:
            - name: tmp
              mountPath: /tmp   # necesario para readOnlyRootFilesystem

          securityContext:
            readOnlyRootFilesystem: true
            runAsNonRoot: true
            runAsUser: 1000

      volumes:
        - name: tmp
          emptyDir: {}
```

### 4.4 Aplicar

```bash
kubectl apply -f k8s/mi-consumer-deployment.yaml -n mi-namespace

# Verificar que arrancó
kubectl get pods -n mi-namespace -l app=mi-consumer
kubectl logs -n mi-namespace deployment/mi-consumer -f

# Verificar métricas
kubectl port-forward -n mi-namespace deployment/mi-consumer 9090:9090
curl http://localhost:9090/metrics | grep kafka_messages
```

---

## 5. Configuraciones comunes

### Broker con TLS sin autenticación (solo encriptación)

```bash
KAFKA_SECURITY_PROTOCOL=SSL
# Las variables SASL_USERNAME/PASSWORD no se necesitan
```

### Broker PLAINTEXT (sin seguridad — solo dev/staging interno)

```bash
KAFKA_SECURITY_PROTOCOL=PLAINTEXT
# Default — no se necesita configurar nada extra
```

### Múltiples consumers en el mismo namespace

Cada consumer tiene su propio Deployment. Comparten los Secrets de infra
(Kafka, Redis, DB) pero cada uno tiene su propio prefijo de env vars:

```bash
# Consumer de pagos
PAGOS_TOPIC=payments-events
PAGOS_GROUP_ID=payments-consumer-group

# Consumer de notificaciones
NOTIF_TOPIC=notification-events
NOTIF_GROUP_ID=notifications-consumer-group
```

---

## Sin Redis (desactivar idempotencia) {#sin-redis}

Si tu entorno no tiene Redis o no necesitás idempotencia, podés hacer que
`IdempotencyStore` sea un no-op. Editá `src/core/idempotency.py`:

```python
class IdempotencyStore:
    """No-op — idempotencia desactivada."""

    async def claim(self, event_id: str) -> bool:
        return True   # siempre "nuevo"

    async def release(self, event_id: str) -> None:
        pass
```

Y remové `redis` de las dependencias en `pyproject.toml` si querés la imagen
más liviana. **Importante**: sin idempotencia, los handlers deben ser
naturalmente idempotentes (ej: `INSERT ... ON CONFLICT DO NOTHING`).

---

## Sin PostgreSQL (consumer sin persistencia)

Si el consumer no necesita guardar datos (ej: solo reenvía a otro topic o
llama una API), no instanciés `Database` en el consumer. El `asyncpg` está
en las deps de runtime pero no se conecta a nada si no lo inicializás.

Simplemente no setees `DATABASE_URL` y no llamés `Database()` en tu consumer.

---

## Checklist de deploy en infra existente

- [ ] `KAFKA_BOOTSTRAP_SERVERS` apunta al cluster correcto
- [ ] `REDIS_URL` disponible (o idempotencia desactivada si no aplica)
- [ ] `DATABASE_URL` configurado (si el consumer usa DB)
- [ ] Topic existe en Kafka y el consumer tiene permisos de lectura
- [ ] DLQ topic existe (o se crea automáticamente en el broker)
- [ ] Consumer group no colisiona con otro consumer en producción
- [ ] `ENVIRONMENT=production` (activa JSON logging)
- [ ] Imagen verificada: FastAPI ausente
- [ ] Health check responde en los primeros 20s
- [ ] Métricas visibles en `/metrics:9090`
- [ ] Logs visibles en el agregador (Datadog, Loki, etc.)
- [ ] Rollout escalonado: `kubectl rollout status deployment/mi-consumer`
