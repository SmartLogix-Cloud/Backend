package cl.smartlogix.order.streaming;

import cl.smartlogix.order.service.OrderTrackingService;
import cl.smartlogix.order.streaming.event.ShipmentEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumidor Kafka (adaptador de infraestructura) de los eventos de envio.
 *
 * <p>Consumer group {@code ms-order-shipment-tracking}: con 3 particiones y
 * concurrency=3 cada hilo atiende una particion; si se escalan replicas de ms-order,
 * Kafka redistribuye las particiones entre ellas (balanceo de carga).
 * Es un grupo DISTINTO al de ms-analytics, por lo que ambos reciben todos los eventos.
 *
 * <p>Solo traduce el mensaje y delega: la logica de negocio vive en
 * {@link OrderTrackingService}.
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class ShipmentEventsConsumer {

    private final OrderTrackingService trackingService;
    private final EventValidator validator;

    @KafkaListener(
        id = "order-shipment-tracking",
        topics = "${smartlogix.kafka.topics.shipment-events}",
        groupId = "${smartlogix.kafka.groups.shipment-tracking}",
        containerFactory = "shipmentEventsListenerFactory")
    public void onShipmentEvent(ConsumerRecord<String, ShipmentEvent> record) {
        MDC.put("kafkaTopic", record.topic());
        MDC.put("kafkaPartition", String.valueOf(record.partition()));
        MDC.put("kafkaOffset", String.valueOf(record.offset()));
        try {
            ShipmentEvent event = validator.validate(record.value());
            MDC.put("eventId", event.eventId());
            trackingService.applyShipmentEvent(event);
            log.info("Procesado {} | envio={} pedido={} estado={} partition={} offset={}",
                event.eventType(), event.shipmentId(), event.orderId(), event.newStatus(),
                record.partition(), record.offset());
        } finally {
            MDC.remove("kafkaTopic");
            MDC.remove("kafkaPartition");
            MDC.remove("kafkaOffset");
            MDC.remove("eventId");
        }
    }
}
