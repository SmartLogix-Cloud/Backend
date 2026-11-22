package cl.smartlogix.order.streaming;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Declaracion de los topicos Kafka que ms-order PRODUCE.
 *
 * <p>Al arrancar, {@code KafkaAdmin} crea el topico si no existe (idempotente),
 * con la misma configuracion que {@code infra/kafka/create-topics.sh}:
 * <ul>
 *   <li>3 particiones: hasta 3 consumidores en paralelo por consumer group; la clave
 *       (id del pedido) garantiza el orden de los eventos de un mismo pedido.</li>
 *   <li>3 replicas + min.insync.replicas=2: con acks=all se tolera la caida de 1 broker.</li>
 *   <li>cleanup.policy=delete con 7 dias de retencion: historial completo de eventos
 *       que permite re-procesar (replay) la ultima semana.</li>
 * </ul>
 */
@Configuration
public class KafkaTopicsConfig {

    public static final String DLT_SUFFIX = ".DLT";
    private static final int PARTITIONS = 3;
    private static final int REPLICAS = 3;

    @Bean
    NewTopic orderEventsTopic(@Value("${smartlogix.kafka.topics.order-events}") String name) {
        return TopicBuilder.name(name)
            .partitions(PARTITIONS)
            .replicas(REPLICAS)
            .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE)
            .config(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(7L * 24 * 60 * 60 * 1000))
            .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")
            .config(TopicConfig.COMPRESSION_TYPE_CONFIG, "producer")
            .build();
    }

    /** DLT de los eventos de envio que ms-order no pudo procesar (retencion 14 dias). */
    @Bean
    NewTopic shipmentEventsDltTopic(@Value("${smartlogix.kafka.topics.shipment-events}") String name) {
        return TopicBuilder.name(name + DLT_SUFFIX)
            .partitions(PARTITIONS)
            .replicas(REPLICAS)
            .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE)
            .config(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(14L * 24 * 60 * 60 * 1000))
            .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")
            .build();
    }
}
