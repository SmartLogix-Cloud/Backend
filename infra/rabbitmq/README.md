# RabbitMQ — clúster de 2 nodos

| Archivo | Uso |
|---|---|
| `docker-compose.node.yml` | **Un** nodo del clúster por instancia (despliegue AWS: `rabbit1` en EC2-mq1 y `rabbit2` en EC2-mq2). |
| `docker-compose.cluster-local.yml` | Los 2 nodos en una sola máquina, para pruebas locales. |
| `rabbitmq.conf` | Formación del clúster (`classic_config`), puerto fijo de distribución Erlang y `autoheal`. |

## Despliegue en 2 instancias (AWS)

1. **Security Group** (entre las EC2 de la VPC, con origen el mismo SG):
   `4369` (epmd), `25672` (distribución Erlang), `5672` (AMQP, también desde la EC2-app) y `15672` (UI de administración).
2. En **cada** EC2 de mensajería:
   ```bash
   cd infra/rabbitmq
   cp .env.node.example .env      # rabbit1/rabbit2, IP privada del par y la MISMA cookie en ambos
   docker compose -f docker-compose.node.yml up -d
   ```
   Hay que arrancar ambos nodos más o menos a la vez: cada uno busca a su par durante ~5 min (`discovery_retry_*`).
3. Verificar:
   ```bash
   docker exec smartlogix-rabbit1 rabbitmqctl cluster_status   # Running Nodes: rabbit@rabbit1, rabbit@rabbit2
   ```
4. En la EC2-app, `RABBITMQ_ADDRESSES=<ip-mq1>:5672,<ip-mq2>:5672`. Spring AMQP se conecta al primer nodo disponible y se reconecta al otro si el primero cae.

Si un nodo arrancó solo (por ejemplo, porque el par no estaba listo), se une manualmente así:
```bash
docker exec smartlogix-rabbit2 rabbitmqctl stop_app
docker exec smartlogix-rabbit2 rabbitmqctl reset
docker exec smartlogix-rabbit2 rabbitmqctl join_cluster rabbit@rabbit1
docker exec smartlogix-rabbit2 rabbitmqctl start_app
```

## Topología (la declaran ms-order y ms-shipping al arrancar)

| Exchange | Tipo | Uso |
|---|---|---|
| `smartlogix.logistics` | **topic** | Eventos de negocio (`order.*`, `shipment.*`). |
| `smartlogix.logistics.dlx` | **direct** | Dead-letter exchange: reenvía cada mensaje fallido a su DLQ por routing key exacta. |

| Cola | Routing key | DLQ |
|---|---|---|
| `shipping.order-ready.q` | `order.ready-for-shipment` | `shipping.order-ready.dlq` |
| `shipping.order-cancelled.q` | `order.cancelled` | `shipping.order-cancelled.dlq` |
| `order.shipment-delivered.q` | `shipment.delivered` | `order.shipment-delivered.dlq` |
| `order.shipment-lost.q` | `shipment.lost` | `order.shipment-lost.dlq` |

> Si un broker local ya tenía `smartlogix.logistics.dlx` declarado como `topic` (versión anterior), hay que borrarlo (o el volumen) antes de arrancar: RabbitMQ no permite redeclarar un exchange con otro tipo.

**Nota sobre alta disponibilidad:** en RabbitMQ 4 las colas *classic* viven en un solo nodo. El clúster distribuye conexiones, colas y metadatos, y sobrevive a la caída de un nodo para las colas del otro. Para replicar el contenido de una cola se usan *quorum queues*, que con 2 nodos no toleran fallos porque necesitan mayoría: se requiere un tercer nodo.
