# Deployment

## Docker — build de producción

El `Dockerfile` es multi-stage con layer caching optimizado:

```bash
# Build
docker build -t kafka-consumer-template:latest .

# Verificar que FastAPI NO está en la imagen
docker run --rm kafka-consumer-template:latest \
  python -c "import fastapi" 2>&1 \
  && echo "FAIL: fastapi encontrado" \
  || echo "OK: fastapi ausente"

# Verificar que tools/ NO está en la imagen
docker run --rm kafka-consumer-template:latest ls /app/tools 2>&1 \
  | grep -q "No such" && echo "OK: tools/ ausente" || echo "FAIL"

# Correr el example consumer
docker run --rm \
  -e KAFKA_BOOTSTRAP_SERVERS=host.docker.internal:9092 \
  -e DATABASE_URL=postgresql://kafka:kafka@host.docker.internal:5432/kafka_consumer \
  -e REDIS_URL=redis://host.docker.internal:6379/0 \
  kafka-consumer-template:latest \
  example-consumer
```

### Estructura del Dockerfile

```
Stage 1 — builder
  ├── COPY pyproject.toml uv.lock   ← layer cacheada (cambia poco)
  ├── uv sync --frozen --no-dev     ← instala solo prod deps
  └── COPY src/                     ← layer cacheada (cambia más)

Stage 2 — runtime
  ├── FROM python:3.13-slim
  ├── COPY --from=builder /app/.venv
  ├── COPY src/                     ← solo src/, sin tools/
  └── USER 1000                     ← non-root
```

La separación en capas hace que cambiar código en `src/` no invalide la
capa de dependencias. Una build típica después de un cambio de código tarda
10-15 segundos en lugar de 2-3 minutos.

### Tageo de imágenes

```bash
# Convencion: semver o git SHA
docker build -t kafka-consumer-template:1.2.3 .
docker build -t kafka-consumer-template:$(git rev-parse --short HEAD) .

# Push a registry
docker tag kafka-consumer-template:1.2.3 gcr.io/mi-proyecto/kafka-consumer-template:1.2.3
docker push gcr.io/mi-proyecto/kafka-consumer-template:1.2.3
```

---

## Docker Compose — infra local completa

```bash
# Solo infra (Redpanda + Redis + Postgres + Console)
docker compose up -d

# Infra + consumer en Docker
docker compose --profile full up

# Ver logs del consumer
docker compose logs example-consumer -f

# Escalar (para probar rebalance)
docker compose --profile full up --scale example-consumer=2

# Bajar todo
docker compose down

# Bajar y borrar volúmenes (reset completo de datos)
docker compose down -v
```

---

## Kubernetes

### Deployment

El `k8s/deployment.yaml` incluye todas las configuraciones de producción:

- **Non-root user**: `securityContext.runAsUser: 1000`
- **Read-only filesystem**: `securityContext.readOnlyRootFilesystem: true`
- **tmpfs para healthcheck**: `/tmp` montado como `emptyDir` para que el
  archivo de healthcheck pueda escribirse con filesystem read-only
- **Liveness probe**: exec contra `/tmp/healthcheck`
- **Resources**: limits y requests configurados
- **Replicas**: 2 por default

```bash
# Aplicar en el namespace de tu equipo
kubectl apply -f k8s/ -n mi-namespace

# Ver estado
kubectl get pods -n mi-namespace -l app=example-consumer
kubectl logs -n mi-namespace deployment/example-consumer -f

# Ver métricas del pod
kubectl port-forward -n mi-namespace pod/<nombre-pod> 9090:9090
curl http://localhost:9090/metrics
```

### Crear un Deployment para un consumer nuevo

```bash
# Copiar el template
cp k8s/deployment.yaml k8s/mi-consumer-deployment.yaml

# Editar los siguientes campos:
# - metadata.name
# - spec.selector.matchLabels.app
# - spec.template.metadata.labels.app
# - spec.template.spec.containers[0].name
# - spec.template.spec.containers[0].image
# - spec.template.spec.containers[0].command  (el entry point del consumer)
# - env vars con el prefijo correcto (WHATSAPP_, PAYMENTS_, etc.)
```

### Secrets en K8s

```bash
# Credenciales Kafka (Redpanda Cloud)
kubectl create secret generic kafka-credentials \
  -n mi-namespace \
  --from-literal=username=mi_usuario \
  --from-literal=password=mi_contraseña

# Base de datos
kubectl create secret generic db-credentials \
  -n mi-namespace \
  --from-literal=url="postgresql://user:pass@host:5432/db"

# Redis
kubectl create secret generic redis-credentials \
  -n mi-namespace \
  --from-literal=url="redis://:password@host:6379/0"
```

Los secrets se referencian en el Deployment como `secretKeyRef` (ver `k8s/deployment.yaml`).

### Health check en K8s

El liveness probe está configurado en `k8s/deployment.yaml`:

```yaml
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
```

Si el consumer se cuelga y deja de escribir el archivo por > 60s, K8s lo
reinicia automáticamente.

### Annotations para scraping de métricas

```yaml
# En spec.template.metadata.annotations:
annotations:
  prometheus.io/scrape: "true"
  prometheus.io/port: "9090"
  prometheus.io/path: "/metrics"
```

Prometheus o el Prometheus Operator del clúster recolecta las métricas
automáticamente con estas annotations.

---

## Migraciones en producción

Las migraciones se corren con Alembic **antes** de actualizar los pods del consumer.

```bash
# En CI/CD, antes del deploy:
docker run --rm \
  -e DATABASE_URL=postgresql://user:pass@host:5432/db \
  kafka-consumer-template:latest \
  python -m alembic upgrade head
```

O como un K8s Job:

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: kafka-consumer-migrate
spec:
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: migrate
          image: kafka-consumer-template:latest
          command: ["python", "-m", "alembic", "upgrade", "head"]
          env:
            - name: DATABASE_URL
              valueFrom:
                secretKeyRef:
                  name: db-credentials
                  key: url
```

### Crear una migración

```bash
# Editar los modelos Alembic o agregar SQL manual
uv run alembic revision --autogenerate -m "add greetings table"
# Revisar el archivo generado en alembic/versions/
uv run alembic upgrade head        # aplicar en local

# Verificar el estado actual
uv run alembic current
uv run alembic history
```

---

## Variables de entorno en producción

Diferencias respecto al desarrollo local:

```bash
ENVIRONMENT=production             # activa JSON logging
LOG_LEVEL=INFO                     # no DEBUG en producción
KAFKA_SECURITY_PROTOCOL=SASL_SSL   # encriptado
KAFKA_BOOTSTRAP_SERVERS=<brokers-redpanda-cloud>
KAFKA_SASL_USERNAME=<desde-secret>
KAFKA_SASL_PASSWORD=<desde-secret>
DATABASE_URL=<desde-secret>
REDIS_URL=<desde-secret>
METRICS_PORT=9090
METRICS_ENABLED=true
HEALTH_FILE_PATH=/tmp/healthcheck  # con tmpfs montado
```

---

## Checklist de deploy

- [ ] Tests unitarios pasan: `uv run pytest tests/unit/`
- [ ] Type check limpio: `uv run mypy src/`
- [ ] Linting limpio: `uv run ruff check src/ tests/`
- [ ] Imagen construida: `docker build -t ... .`
- [ ] FastAPI ausente de la imagen (verificación arriba)
- [ ] Migraciones aplicadas antes del rollout
- [ ] Secrets creados en K8s
- [ ] Health check configurable via `HEALTH_FILE_PATH`
- [ ] Métricas scrapeadas por Prometheus (annotations en Deployment)
- [ ] Rollout escalonado: `kubectl rollout status deployment/...`
