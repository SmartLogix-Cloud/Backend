package cl.smartlogix.shipping.streaming;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Declaracion del topico Kafka que ms-shipping PRODUCE.
 *
 * <p>{@code smartlogix.shipment-events}: 3 particiones, 3 replicas,
 * min.insync.replicas=2 y cleanup.policy=compact,delete. La clave es el id del envio:
 * la compactacion conserva el ultimo estado de cada envio (un consumidor nuevo puede
 * reconstruir el estado actual de todos los envios) y la retencion de 30 dias purga
 * el historial antiguo.
 */
@Configuration
public class KafkaTopicsConfig {

    @Bean
    NewTopic shipmentEventsTopic(@Value("${smartlogix.kafka.topics.shipment-events}") String name) {
        return TopicBuilder.name(name)
            .partitions(3)
            .replicas(3)
            .config(TopicConfig.CLEANUP_POLICY_CONFIG,
                TopicConfig.CLEANUP_POLICY_COMPACT + "," + TopicConfig.CLEANUP_POLICY_DELETE)
            .config(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(30L * 24 * 60 * 60 * 1000))
            .config(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, "60000")
            .config(TopicConfig.SEGMENT_MS_CONFIG, String.valueOf(24L * 60 * 60 * 1000))
            .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")
            .config(TopicConfig.COMPRESSION_TYPE_CONFIG, "producer")
            .build();
    }
}
