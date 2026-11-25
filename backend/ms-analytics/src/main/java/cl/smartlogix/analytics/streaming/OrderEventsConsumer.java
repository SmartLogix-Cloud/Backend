package cl.smartlogix.analytics.streaming;

import cl.smartlogix.analytics.service.LogisticsMetricsService;
import cl.smartlogix.analytics.streaming.event.OrderEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumidor de {@code smartlogix.order-events} (consumer group {@code ms-analytics-orders}).
 * Adaptador de infraestructura: valida y delega en {@link LogisticsMetricsService}.
 */
@Component
@Slf4j
public class OrderEventsConsumer extends ReplayableConsumer {

    private final LogisticsMetricsService metrics;
    private final EventValidator validator;

    public OrderEventsConsumer(LogisticsMetricsService metrics, EventValidator validator,
                               @Value("${smartlogix.analytics.replay-on-startup:true}") boolean replay) {
        super(replay);
        this.metrics = metrics;
        this.validator = validator;
    }

    @KafkaListener(
        id = "analytics-orders",
        topics = "${smartlogix.kafka.topics.order-events}",
        groupId = "${smartlogix.kafka.groups.orders}",
        containerFactory = "orderEventsListenerFactory")
    public void onOrderEvent(ConsumerRecord<String, OrderEvent> record) {
        MDC.put("kafkaTopic", record.topic());
        MDC.put("kafkaPartition", String.valueOf(record.partition()));
        MDC.put("kafkaOffset", String.valueOf(record.offset()));
        try {
            OrderEvent event = validator.validate(record.value());
            boolean applied = metrics.applyOrderEvent(event, record.partition(), record.offset());
            log.debug("{} {} | pedido={} estado={} partition={} offset={}",
                applied ? "Aplicado" : "Ignorado (duplicado/antiguo)", event.eventType(),
                event.orderId(), event.newStatus(), record.partition(), record.offset());
        } finally {
            MDC.remove("kafkaTopic");
            MDC.remove("kafkaPartition");
            MDC.remove("kafkaOffset");
        }
    }
}
