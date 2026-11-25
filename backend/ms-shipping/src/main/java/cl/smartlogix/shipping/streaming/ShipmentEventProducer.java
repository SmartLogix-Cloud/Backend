package cl.smartlogix.shipping.streaming;

import cl.smartlogix.shipping.model.Shipment;
import cl.smartlogix.shipping.model.ShipmentState;
import cl.smartlogix.shipping.streaming.event.ShipmentEvent;
import cl.smartlogix.shipping.streaming.event.ShipmentEventType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * Productor Kafka del dominio Envio: publica cada hito de seguimiento en
 * {@code smartlogix.shipment-events}. Lo consumen ms-order (proyeccion de
 * seguimiento del pedido) y ms-analytics (metricas en tiempo real), cada uno en su
 * propio consumer group.
 *
 * <p>RabbitMQ sigue transportando solo los hitos que cambian la maquina de estados
 * del pedido (entregado / perdido); Kafka transporta el flujo completo de tracking.
 */
@Component
@Slf4j
public class ShipmentEventProducer {

    private static final String SOURCE = "ms-shipping";

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String topic;

    public ShipmentEventProducer(KafkaTemplate<String, Object> kafkaTemplate,
                                 @Value("${smartlogix.kafka.topics.shipment-events}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    public void publishCreated(Shipment shipment, String comment) {
        publish(toEvent(shipment, ShipmentEventType.SHIPMENT_CREATED, null, null, comment));
    }

    public void publishCarrierAssigned(Shipment shipment) {
        publish(toEvent(shipment, ShipmentEventType.CARRIER_ASSIGNED, shipment.getStatus(), null,
            "Transportista asignado"));
    }

    public void publishStatusChanged(Shipment shipment, ShipmentState previous, String location, String comment) {
        publish(toEvent(shipment, ShipmentEventType.SHIPMENT_STATUS_CHANGED, previous, location, comment));
    }

    private ShipmentEvent toEvent(Shipment s, ShipmentEventType type, ShipmentState previous,
                                  String location, String comment) {
        return new ShipmentEvent(
            UUID.randomUUID().toString(),
            type,
            s.getId(),
            s.getOrderId(),
            s.getTrackingNumber(),
            s.getCarrier() != null ? s.getCarrier().getName() : null,
            previous != null ? previous.name() : null,
            s.getStatus().name(),
            location,
            comment,
            OffsetDateTime.now().toString(),
            SOURCE);
    }

    /** Publica tras el commit de la transaccion (nunca eventos de cambios revertidos). */
    private void publish(ShipmentEvent event) {
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

    private void send(ShipmentEvent event) {
        String key = String.valueOf(event.shipmentId());
        try {
            kafkaTemplate.send(topic, key, event).whenComplete((result, ex) -> {
                if (ex != null) {
                    log.error("Fallo publicando {} del envio {} en {}: {}",
                        event.eventType(), event.shipmentId(), topic, ex.getMessage());
                } else {
                    var md = result.getRecordMetadata();
                    log.info("Evento Kafka publicado {} | envio={} pedido={} estado={} topic={} partition={} offset={}",
                        event.eventType(), event.shipmentId(), event.orderId(), event.newStatus(),
                        md.topic(), md.partition(), md.offset());
                }
            });
        } catch (RuntimeException ex) {
            log.error("No se pudo enviar {} del envio {} a Kafka: {}",
                event.eventType(), event.shipmentId(), ex.getMessage());
        }
    }
}
