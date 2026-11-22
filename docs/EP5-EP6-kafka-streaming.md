# EP5 / EP6 — Streaming con Kafka en SmartLogix

## 1. Qué se agregó y por qué Kafka complementa a RabbitMQ

| | RabbitMQ (EP3, se mantiene) | Kafka (EP5) |
|---|---|---|
| Rol | **Orquestación**: comandos entre ms-order y ms-shipping que cambian la máquina de estados (`order.ready-for-shipment`, `order.cancelled`, `shipment.delivered`, `shipment.lost`). | **Streaming y analítica**: el historial completo de hechos de pedidos y envíos, disponible para cualquier suscriptor. |
| Entrega | Mensaje a una cola: se consume una vez y desaparece. ACK manual y DLQ. | Log persistente y particionado: cada consumer group lee todo a su ritmo, con retención y *replay*. |
| Por qué | Ruteo flexible (topic), confirmación por mensaje y DLQ por cola. | Alto volumen, orden por clave, varios consumidores independientes y reconstrucción de estado. |

Las integraciones existentes con RabbitMQ **no se modificaron** (salvo el DLX, que pasó a `direct`; ver `infra/rabbitmq/README.md`). Kafka se publica **además**, y después del commit de la transacción.

## 2. Flujo de eventos

```
                       RabbitMQ (comandos)                          
  ms-order ── order.ready-for-shipment ──▶ ms-shipping (crea envío)
  ms-order ◀── shipment.delivered/lost ──── ms-shipping

                       Kafka (streaming)
  ms-order ───────▶ smartlogix.order-events ─────▶ ms-analytics  [ms-analytics-orders]
  ms-shipping ────▶ smartlogix.shipment-events ─┬▶ ms-order      [ms-order-shipment-tracking]  → proyección pedido_seguimiento
                                                └▶ ms-analytics  [ms-analytics-shipments]      → métricas en tiempo real
                                         (fallos) ─▶ *.DLT
  ms-analytics ──▶ REST /analytics/* y SSE /analytics/stream ──▶ BFF ──▶ API Gateway ──▶ Frontend "Streaming en Vivo"
```

**Casos de uso que necesitan streaming:**

1. **Seguimiento del pedido en tiempo real** (`GET /orders/{id}/tracking`). ms-order mantiene la tabla `pedido_seguimiento` con el último estado logístico de cada pedido, alimentada por `shipment-events`. Así evita consultar ms-shipping de forma síncrona. El tópico es compactado y conserva el último estado por envío.
2. **Analítica operacional** (`/analytics/summary`): pedidos por estado y tipo, ventas activas, envíos por estado, incidencias (perdido, retrasado, entrega fallida) y tasa de entrega. Se recalculan con cada evento. Como las métricas viven en memoria, al reiniciar se **re-leen los tópicos desde el inicio**, dentro de la ventana de retención.
3. **Feed de eventos**: los últimos eventos con su tópico, partición y offset, que muestra la vista del frontend.

## 3. Diseño de los eventos

**`OrderEvent`** (`smartlogix.order-events`, clave = `orderId`)

| Campo | Por qué existe |
|---|---|
| `eventId` (UUID) | Idempotencia: los consumidores descartan duplicados. |
| `eventType` | `ORDER_CREATED` o `ORDER_STATUS_CHANGED`. |
| `orderId`, `orderCode` | Identidad del pedido. `orderId` es la clave de particionado y garantiza el orden por pedido. |
| `customerId`, `orderType` | Métricas por cliente y por tipo (ESTANDAR / EXPRESS). |
| `previousStatus`, `newStatus` | Transición exacta: el consumidor actualiza su proyección sin consultar a ms-order. |
| `total`, `itemCount` | Métricas de ventas sin llamar a ms-order. |
| `reason` | Motivo de la transición (auditoría). |
| `occurredAt` (ISO-8601) | Detectar eventos fuera de orden. |
| `source` | Trazabilidad del emisor. |

**`ShipmentEvent`** (`smartlogix.shipment-events`, clave = `shipmentId`): `eventId`, `eventType` (`SHIPMENT_CREATED`, `CARRIER_ASSIGNED`, `SHIPMENT_STATUS_CHANGED`), `shipmentId`, `orderId` (para que ms-order proyecte por pedido), `trackingNumber`, `carrierName`, `previousStatus`, `newStatus`, `location`, `comment`, `occurredAt` y `source`.

Los eventos son *records* Java (DTO) con anotaciones de Bean Validation. Se serializan con un `Serializer` JSON propio (`JsonEventSerializer`) y se deserializan de forma **tipada** (`JsonEventDeserializer<T>`) envuelta en `ErrorHandlingDeserializer`. Los campos desconocidos se ignoran, de modo que el esquema puede evolucionar sin romper a los consumidores. Los consumidores reciben los estados como texto, sin acoplarse a los enums del productor.

## 4. Configuración del cliente Kafka

**Productor** (ms-order y ms-shipping): `acks=all`, `enable.idempotence=true`, `retries=10`, `compression.type=lz4`, `linger.ms=10`, `batch.size=16384`, `delivery.timeout.ms=30000` y `max.block.ms=5000` (una petición HTTP nunca queda colgada si Kafka no responde). Publica en `afterCommit`: nunca se emite el evento de un cambio que hizo rollback.

**Consumidor**: `enable.auto.commit=false` con `AckMode.RECORD` (commit por registro), `auto.offset.reset=earliest`, `isolation.level=read_committed`, `CooperativeStickyAssignor` (rebalanceo incremental), `concurrency=3` (un hilo por partición) y timeouts de sesión, heartbeat y poll explícitos.

**Consumer groups**:
- Dos grupos **distintos** sobre `shipment-events` (`ms-order-shipment-tracking` y `ms-analytics-shipments`): ambos reciben **todos** los eventos (publish/subscribe) y cada microservicio cumple su responsabilidad.
- **Dentro** de cada grupo, las 3 particiones se reparten entre los consumidores (balanceo). Si se escala ms-order a 2 réplicas, Kafka redistribuye las particiones entre ellas.

## 5. Manejo de errores (probado)

| Caso | Tratamiento | Resultado de la prueba |
|---|---|---|
| Payload no es JSON | `ErrorHandlingDeserializer` → directo al DLT | `key=999 → DLT (JsonParseException)` |
| Evento inválido (Bean Validation) | `InvalidEventException`, no reintentable → DLT | `key=997 → DLT (newStatus no debe estar vacío)` |
| Referencia inexistente (pedido que no existe) | `InvalidEventException` en ms-order → DLT. ms-analytics lo acepta porque no depende de ms-order | `key=998 → DLT solo en ms-order` |
| Fallo transitorio (BD caída, timeout) | `DefaultErrorHandler` con `ExponentialBackOff` (3 reintentos: 1 s, 2 s, 4 s) y log de cada intento; luego DLT | — |
| Duplicado / fuera de orden | Idempotencia por `eventId` y comparación de `occurredAt` | Tests unitarios |

El DLT conserva la partición original y headers con la excepción, el tópico y el offset de origen. Todos los logs incluyen tópico, partición, offset, clave y causa, y además se cargan en el MDC.

## 6. Administración de Kafka (ms-broker-admin)

Swagger: `http://localhost:8095/swagger-ui.html` (en AWS, vía túnel SSH).

| Método | Ruta | Operación |
|---|---|---|
| GET | `/kafka/cluster` | Id del clúster, controller y brokers |
| GET | `/kafka/topics` | Listar tópicos (particiones y réplicas) |
| GET | `/kafka/topics/{name}` | Líder, réplicas, ISR y offsets por partición; mensajes retenidos; configuración explícita |
| POST | `/kafka/topics` | Crear tópico con particiones, replicación y configs |
| PUT | `/kafka/topics/{name}/configs` | Modificar o eliminar configs (`retention.ms`, `cleanup.policy`, …) |
| POST | `/kafka/topics/{name}/partitions` | Aumentar particiones |
| DELETE | `/kafka/topics/{name}` | Eliminar tópico |
| GET | `/kafka/consumer-groups` | Listar grupos y su estado |
| GET | `/kafka/consumer-groups/{id}` | Miembros, asignación de particiones, offsets y **lag** |
| POST | `/kafka/consumer-groups/{id}/reset-offsets` | `EARLIEST` / `LATEST` / `TO_OFFSET`, para reprocesar |
| DELETE | `/kafka/consumer-groups/{id}` | Eliminar grupo inactivo |

Las mismas operaciones de RabbitMQ siguen en `/queues`, `/exchanges` y `/bindings`.

## 7. Despliegue en AWS

| Instancia | Contenido | Tamaño sugerido |
|---|---|---|
| `smartlogix-backend` (existente) | BFF, 6 microservicios y 4 Postgres (`docker-compose.aws.yml`) | t3.large |
| `smartlogix-mq1` | `rabbit1` + Kafka (3 ZK + 3 brokers + Kafka UI) | t3.large |
| `smartlogix-mq2` | `rabbit2` | t3.small |

Puertos del Security Group de las EC2 de mensajería, con origen el SG de la VPC: 4369, 25672, 5672, 15672 (RabbitMQ) y 9092-9094 (Kafka). Kafka UI (8088) y la UI de RabbitMQ (15672) quedan abiertas solo hacia la IP de quien presenta.

En la EC2-app (`.env`):
```
RABBITMQ_ADDRESSES=<ip-privada-mq1>:5672,<ip-privada-mq2>:5672
KAFKA_BOOTSTRAP_SERVERS=<ip-privada-mq1>:9092,<ip-privada-mq1>:9093,<ip-privada-mq1>:9094
```

El API Gateway HTTP existente ya enruta `/{proxy+}` con JWT hacia el BFF, por lo que `/analytics/*` no requiere rutas nuevas.

## 8. Guion de demostración (EP6)

1. **Kafka UI** → *Brokers*: 3 brokers y el controller. *Topics*: `order-events` y `shipment-events` con 3 particiones, 3 réplicas e ISR completo. *Settings*: `retention.ms` y `cleanup.policy` de cada uno.
2. **Frontend → "Streaming en Vivo"** abierto en otra ventana.
3. Crear un pedido, aprobarlo y pasarlo a `EN_PREPARACION`. Por RabbitMQ, ms-shipping crea el envío; por Kafka, aparecen en vivo `ORDER_CREATED`, `ORDER_STATUS_CHANGED` y `SHIPMENT_CREATED`, cada uno con su partición y offset.
4. Avanzar el envío: `RETIRADO`, `EN_TRANSITO` y `ENTREGADO`. Las métricas cambian solas, el pedido pasa a `FINALIZADO` (RabbitMQ) y `GET /orders/{id}/tracking` muestra la proyección (Kafka).
5. **Kafka UI → Consumers**: los 3 grupos, cada uno con 3 miembros y 1 partición por miembro, y lag en 0. Detener ms-analytics, generar eventos y mostrar cómo **crece el lag**; al levantarlo de nuevo, el lag vuelve a 0.
6. **Tolerancia a fallos**: `docker stop smartlogix-kafka2`. El ISR baja a 2 y se reasignan los líderes, pero los pedidos siguen publicándose. `docker start smartlogix-kafka2` y el ISR se recupera.
7. **DLT**: publicar un mensaje inválido desde Kafka UI (*Produce message*) y mostrarlo en `smartlogix.shipment-events.DLT` con los headers de la excepción.
8. **RabbitMQ**: `rabbitmqctl cluster_status` (2 nodos en 2 EC2), las 4 colas con su DLQ y los exchanges `topic` y `direct`.
