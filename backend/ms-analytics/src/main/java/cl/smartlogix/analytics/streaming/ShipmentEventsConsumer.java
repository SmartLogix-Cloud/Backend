package cl.smartlogix.analytics.streaming;

import cl.smartlogix.analytics.service.LogisticsMetricsService;
import cl.smartlogix.analytics.streaming.event.ShipmentEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumidor de {@code smartlogix.shipment-events} (consumer group {@code ms-analytics-shipments}).
 * Es un grupo distinto al de ms-order ({@code ms-order-shipment-tracking}): ambos
 * reciben TODOS los eventos del topico (publish/subscribe entre grupos), mientras
 * que dentro de cada grupo las particiones se reparten (balanceo de carga).
 */
@Component
@Slf4j
public class ShipmentEventsConsumer extends ReplayableConsumer {

    private final LogisticsMetricsService metrics;
    private final EventValidator validator;

    public ShipmentEventsConsumer(LogisticsMetricsService metrics, EventValidator validator,
                                  @Value("${smartlogix.analytics.replay-on-startup:true}") boolean replay) {
        super(replay);
        this.metrics = metrics;
        this.validator = validator;
    }

    @KafkaListener(
        id = "analytics-shipments",
        topics = "${smartlogix.kafka.topics.shipment-events}",
        groupId = "${smartlogix.kafka.groups.shipments}",
        containerFactory = "shipmentEventsListenerFactory")
    public void onShipmentEvent(ConsumerRecord<String, ShipmentEvent> record) {
        MDC.put("kafkaTopic", record.topic());
        MDC.put("kafkaPartition", String.valueOf(record.partition()));
        MDC.put("kafkaOffset", String.valueOf(record.offset()));
        try {
            ShipmentEvent event = validator.validate(record.value());
            boolean applied = metrics.applyShipmentEvent(event, record.partition(), record.offset());
            log.debug("{} {} | envio={} pedido={} estado={} partition={} offset={}",
                applied ? "Aplicado" : "Ignorado (duplicado/antiguo)", event.eventType(),
                event.shipmentId(), event.orderId(), event.newStatus(), record.partition(), record.offset());
        } finally {
            MDC.remove("kafkaTopic");
            MDC.remove("kafkaPartition");
            MDC.remove("kafkaOffset");
        }
    }
}
