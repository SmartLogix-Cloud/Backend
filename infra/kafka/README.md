# Kafka — plataforma de streaming de SmartLogix

`docker-compose.yml` levanta una arquitectura Kafka con **3 nodos Zookeeper**, **3 brokers**, un job que crea los tópicos (`kafka-init`) y **Kafka UI**.

```
                 ┌────────────── Zookeeper ensemble ──────────────┐
                 │  zookeeper1      zookeeper2      zookeeper3    │  quorum 2 de 3
                 └───────▲───────────────▲───────────────▲────────┘
                         │ metadatos, elección del controller
   ms-order  ──┐   ┌─────┴─────┐   ┌─────┴─────┐   ┌─────┴─────┐
   ms-shipping ├──▶│  kafka1   │◀─▶│  kafka2   │◀─▶│  kafka3   │  replicación de particiones
   ms-analytics┘   │ :9092     │   │ :9093     │   │ :9094     │
                   └───────────┘   └───────────┘   └───────────┘
                         ▲  kafka-ui (:8088): brokers, tópicos, mensajes, consumer groups, lag
```

## Roles y tolerancia a fallos

| Componente | Rol | ¿Qué pasa si cae uno? |
|---|---|---|
| **Zookeeper ×3** | Guarda los metadatos del clúster: brokers vivos, tópicos, configuración y quién es el *controller*. Funciona por quorum (mayoría). | Con 2 de 3 nodos vivos sigue habiendo quorum y el clúster opera normal. Con 1 solo nodo no hay quorum y no se pueden hacer cambios de metadatos. |
| **Broker ×3** | Guardan las particiones. Cada partición tiene un **líder** (atiende lecturas y escrituras) y **seguidores** que replican. Un broker hace además de *controller* y reasigna líderes. | El controller elige un nuevo líder entre las réplicas en sincronía (ISR). Con `min.insync.replicas=2` y `acks=all` las escrituras siguen aceptándose con 2 brokers vivos. **Probado:** con `kafka2` detenido se crearon pedidos y los eventos se publicaron y consumieron. |
| **kafka-init** | Crea los tópicos con su configuración (idempotente, `--if-not-exists`). | Es un job: termina al crear los tópicos. |
| **Kafka UI** | Administración y monitoreo. | No afecta al flujo de datos. |

Configuración de durabilidad del clúster: `default.replication.factor=3`, `min.insync.replicas=2`, `unclean.leader.election.enable=false` (nunca se elige como líder una réplica desactualizada) y `auto.create.topics.enable=false` (los tópicos solo se crean de forma controlada).

## Tópicos

| Tópico | Productor | Consumer groups | Particiones / réplicas | Política | Justificación |
|---|---|---|---|---|---|
| `smartlogix.order-events` | ms-order | `ms-analytics-orders` | 3 / 3 | `delete`, retención 7 días | Historial completo del ciclo de vida del pedido. 7 días permiten reprocesar (*replay*) la última semana para reconstruir métricas. |
| `smartlogix.shipment-events` | ms-shipping | `ms-order-shipment-tracking`, `ms-analytics-shipments` | 3 / 3 | `compact,delete`, retención 30 días | La clave es el id del envío. La compactación conserva **siempre el último estado de cada envío**, así un consumidor nuevo reconstruye el estado actual sin leer todo el historial. |
| `*.DLT` | Error handlers | — (análisis manual) | 3 / 3 | `delete`, retención 14 días | Eventos que fallaron tras los reintentos. Se conservan más tiempo para analizarlos y reprocesarlos. |

**¿Por qué 3 particiones?** La partición es la unidad de paralelismo: dentro de un consumer group, cada partición la atiende un solo consumidor. Con 3 particiones se pueden tener hasta 3 consumidores en paralelo por grupo (los listeners usan `concurrency=3`). La **clave** del mensaje (id del pedido o del envío) garantiza que todos los eventos de una misma entidad caigan en la misma partición y se procesen **en orden**.

**¿Por qué 3 réplicas?** Hay una copia en cada broker. Junto con `min.insync.replicas=2` y `acks=all`, una escritura solo se confirma cuando al menos 2 brokers la tienen: se tolera perder 1 broker sin perder datos ni dejar de aceptar escrituras.

## Uso

```bash
cp .env.example .env          # KAFKA_ADVERTISED_HOST = IP privada de la EC2 (o host.docker.internal en local)
docker compose up -d
docker compose logs kafka-init          # ver la creación de tópicos
```

Kafka UI: `http://<host>:8088` (usuario y clave en `.env`).

### Supervisión por CLI (offsets, lag y distribución del consumo)

```bash
# Detalle de un tópico: líder, réplicas e ISR por partición
docker exec smartlogix-kafka1 kafka-topics --bootstrap-server kafka1:29092 --describe --topic smartlogix.order-events

# Consumer group: offset comprometido, log-end-offset, LAG y qué consumidor atiende cada partición
docker exec smartlogix-kafka1 kafka-consumer-groups --bootstrap-server kafka1:29092 --describe --group ms-order-shipment-tracking

# Ver los mensajes que llegaron al DLT, con sus headers (causa del error)
docker exec smartlogix-kafka1 kafka-console-consumer --bootstrap-server kafka1:29092 \
  --topic smartlogix.shipment-events.DLT --from-beginning --property print.headers=true --timeout-ms 5000
```

- **Offset comprometido**: último mensaje que el grupo confirmó como procesado.
- **Lag** = `log-end-offset − offset comprometido`: mensajes pendientes. Si el lag crece de forma sostenida, los consumidores no dan abasto (conviene escalar, hasta 1 consumidor por partición) o hay un consumidor bloqueado.
- **Distribución**: la columna `CONSUMER-ID` muestra a qué consumidor se asignó cada partición. Si un consumidor cae, el *rebalance* (`CooperativeStickyAssignor`) reasigna solo sus particiones.

Lo mismo está disponible por API en **ms-broker-admin** (`/kafka/...`, ver `docs/EP5-EP6-kafka-streaming.md`).
