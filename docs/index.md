# Documentación — kafka-consumer-template

Guía de navegación para el proyecto. Cada sección es un documento independiente
diseñado para leerse en orden la primera vez y como referencia después.

---

## Por dónde empezar

| Si quieres... | Lee... |
|---|---|
| Entender qué hace y qué NO hace el proyecto | [Alcance](scope.md) |
| Desplegar en infra ya existente (Kafka + Redis) | [Deploy en infra existente](deploy-existing-infra.md) |
| Ejecutar y validar el proyecto paso a paso | [Ejecutar y validar](running-and-validating.md) |
| Entender el diseño interno y decisiones | [Arquitectura](architecture.md) |
| Levantar el entorno local | [Desarrollo local](local-development.md) |
| Crear un nuevo consumer | [Crear un consumer](creating-a-consumer.md) |
| Escribir o correr tests | [Testing](testing.md) |
| Configurar variables de entorno | [Configuración](configuration.md) |
| Entender el manejo de errores | [Manejo de errores](error-handling.md) |
| Ver métricas y logs | [Observabilidad](observability.md) |
| Deployar Docker / K8s (infra desde cero) | [Deployment](deployment.md) |
| Ver qué falta para producción robusta | [TODO](../TODO.md) |

---

## Estructura de documentos

```
docs/
├── index.md                   ← Estás aquí
├── scope.md                   ← Qué cubre, qué no cubre, dependencias requeridas
├── deploy-existing-infra.md   ← Deploy en infra ya existente (Docker / k3s)
├── running-and-validating.md  ← Ejecución y validación paso a paso (13 pasos)
├── architecture.md            ← Diseño del sistema, BaseConsumer, decisiones
├── local-development.md       ← Setup, infra local, dev tools
├── configuration.md           ← Env vars, Settings, prefijos por consumer
├── testing.md                 ← Cómo escribir y correr tests (unit + integration)
├── error-handling.md          ← DomainError, DLQ, retry, filosofía de errores
├── observability.md           ← Logs estructurados, métricas Prometheus, health
├── deployment.md              ← Docker, K8s, infra desde cero
├── creating-a-consumer.md     ← Workflow paso a paso para un consumer nuevo
└── patterns/
    ├── background-tasks.md    ← Trabajo > 30s con crash recovery
    ├── concurrency.md         ← Fair scheduling semaphore
    ├── database.md            ← Bulk insert, SQL identifier validation
    └── idempotency.md         ← Redis SET NX, event_id, TTL

TODO.md                        ← Pendientes para robustez completa (raíz del proyecto)
```

---

## Conceptos clave en 90 segundos

**Consumer**: proceso standalone (no web app) que lee mensajes de un topic Kafka,
los procesa, y commitea el offset solo si el procesamiento fue exitoso.

**BaseConsumer**: clase base que encapsula TODAS las garantías del loop — idempotencia,
retry con jitter, DLQ, ContextVars para tracing. El developer solo implementa el handler.

**Handler**: función async pura. Recibe un evento validado por Pydantic, hace sus
side effects (DB, Redis, API), y retorna. Nunca ve duplicados. Nunca decide retry.
Nunca commitea.

**DLQ (Dead Letter Queue)**: topic separado donde van los mensajes que no pudieron
procesarse (parse error, max retries excedidos, NonRetryableError). El consumer
principal siempre avanza.

**Idempotencia**: Redis SET NX por `event_id`. Si el evento ya fue procesado, el
BaseConsumer lo detecta antes de invocar el handler y salta al siguiente.

**ContextVars**: `message_id`, `consumer_name`, `topic`, `event_type`, `attempt`
se propagan automáticamente a todos los logs generados mientras se procesa un
mensaje, sin pasar parámetros.

---

## Para agentes de IA

Leer `CLAUDE.md` en la raíz del proyecto.
