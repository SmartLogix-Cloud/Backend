package cl.smartlogix.order.streaming;

import cl.smartlogix.order.model.Order;
import cl.smartlogix.order.model.OrderStatus;
import cl.smartlogix.order.streaming.event.OrderEvent;
import cl.smartlogix.order.streaming.event.OrderEventType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * Productor Kafka del dominio Pedido: publica cada hecho del ciclo de vida del
 * pedido en {@code smartlogix.order-events} para streaming y analitica.
 *
 * <p>Complementa (no reemplaza) a RabbitMQ: RabbitMQ sigue orquestando los comandos
 * entre ms-order y ms-shipping; Kafka distribuye el historial de eventos a quien
 * quiera suscribirse (ms-analytics u otros), con retencion y posibilidad de replay.
 *
 * <p>Garantias:
 * <ul>
 *   <li>Clave = id del pedido: todos sus eventos caen en la misma particion (orden).</li>
 *   <li>Se publica DESPUES del commit de la transaccion: nunca se emite un evento de
 *       un cambio que luego hizo rollback.</li>
 *   <li>acks=all + idempotencia (ver application.properties): sin perdidas ni duplicados
 *       por reintentos del productor.</li>
 * </ul>
 */
@Component
@Slf4j
public class OrderEventProducer {

    private static final String SOURCE = "ms-order";

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String topic;

    public OrderEventProducer(KafkaTemplate<String, Object> kafkaTemplate,
                              @Value("${smartlogix.kafka.topics.order-events}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    /** Publica el alta de un pedido (requiere el pedido ya persistido, con id). */
    public void publishCreated(Order order) {
        publish(toEvent(order, OrderEventType.ORDER_CREATED, null, "Pedido registrado"));
    }

    /** Publica una transicion de estado del pedido. */
    public void publishStatusChanged(Order order, OrderStatus previous, String reason) {
        publish(toEvent(order, OrderEventType.ORDER_STATUS_CHANGED, previous, reason));
    }

    private OrderEvent toEvent(Order order, OrderEventType type, OrderStatus previous, String reason) {
        return new OrderEvent(
            UUID.randomUUID().toString(),
            type,
            order.getId(),
            order.getCode(),
            order.getCustomerId(),
            order.getType() != null ? order.getType().name() : null,
            previous != null ? previous.name() : null,
            order.getStatus().name(),
            order.getTotal(),
            order.getItems() != null ? order.getItems().size() : 0,
            reason,
            OffsetDateTime.now().toString(),
            SOURCE);
    }

    private void publish(OrderEvent event) {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    send(event);
                }
            });
        } else {
            send(event);
        }
    }

    private void send(OrderEvent event) {
        String key = String.valueOf(event.orderId());
        try {
            kafkaTemplate.send(topic, key, event).whenComplete((result, ex) -> {
                if (ex != null) {
                    log.error("Fallo publicando {} del pedido {} en {}: {}",
                        event.eventType(), event.orderId(), topic, ex.getMessage());
                } else {
                    var md = result.getRecordMetadata();
                    log.info("Evento Kafka publicado {} | pedido={} topic={} partition={} offset={} eventId={}",
                        event.eventType(), event.orderId(), md.topic(), md.partition(), md.offset(), event.eventId());
                }
            });
        } catch (RuntimeException ex) {
            // Kafka no disponible (p.ej. timeout de metadata): el cambio ya esta
            // confirmado en BD, se registra el fallo sin romper la operacion de negocio.
            log.error("No se pudo enviar {} del pedido {} a Kafka: {}",
                event.eventType(), event.orderId(), ex.getMessage());
        }
    }
}
