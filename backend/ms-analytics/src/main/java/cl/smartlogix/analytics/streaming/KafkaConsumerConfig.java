package cl.smartlogix.analytics.streaming;

import cl.smartlogix.analytics.streaming.event.OrderEvent;
import cl.smartlogix.analytics.streaming.event.ShipmentEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.util.backoff.ExponentialBackOff;

import java.util.HashMap;
import java.util.Map;

/**
 * Infraestructura de consumo Kafka de ms-analytics.
 *
 * <p>Un container factory tipado por topico (deserializacion a DTO), con
 * concurrency=3 (un hilo por particion), commit de offset por registro y una
 * estrategia de errores comun:
 * <ul>
 *   <li>Payload ilegible o evento invalido: directo al DLT, sin reintentos.</li>
 *   <li>Fallo transitorio: reintentos con backoff exponencial y luego DLT.</li>
 * </ul>
 */
@Configuration
@Slf4j
public class KafkaConsumerConfig {

    public static final String DLT_SUFFIX = ".DLT";

    @Value("${smartlogix.kafka.consumer.concurrency:3}")
    private int concurrency;

    // ===================== Dead Letter Topics =====================

    @Bean
    NewTopic orderEventsDltTopic(@Value("${smartlogix.kafka.topics.order-events}") String topic) {
        return dltTopic(topic);
    }

    @Bean
    NewTopic shipmentEventsDltTopic(@Value("${smartlogix.kafka.topics.shipment-events}") String topic) {
        return dltTopic(topic);
    }

    private static NewTopic dltTopic(String topic) {
        return TopicBuilder.name(topic + DLT_SUFFIX)
            .partitions(3)
            .replicas(3)
            .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE)
            .config(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(14L * 24 * 60 * 60 * 1000))
            .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")
            .build();
    }

    // ===================== Manejo de errores =====================

    @Bean
    DefaultErrorHandler kafkaErrorHandler(
            KafkaOperations<?, ?> kafkaTemplate,
            @Value("${smartlogix.kafka.retry.max-retries:3}") int maxRetries,
            @Value("${smartlogix.kafka.retry.initial-interval-ms:1000}") long initialInterval,
            @Value("${smartlogix.kafka.retry.multiplier:2.0}") double multiplier) {

        DeadLetterPublishingRecoverer dltPublisher = new DeadLetterPublishingRecoverer(kafkaTemplate,
            (rec, ex) -> new TopicPartition(rec.topic() + DLT_SUFFIX, rec.partition()));

        ConsumerRecordRecoverer recoverer = (rec, ex) -> {
            log.error("Evento derivado a DLT | topic={} partition={} offset={} key={} causa={}",
                rec.topic(), rec.partition(), rec.offset(), rec.key(), rootMessage(ex));
            dltPublisher.accept(rec, ex);
        };

        ExponentialBackOff backOff = new ExponentialBackOff(initialInterval, multiplier);
        backOff.setMaxAttempts(maxRetries);
        backOff.setMaxInterval(10_000L);

        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff);
        handler.addNotRetryableExceptions(InvalidEventException.class, IllegalArgumentException.class);
        handler.setRetryListeners((rec, ex, attempt) ->
            log.warn("Reintento {}/{} | topic={} partition={} offset={} key={} causa={}",
                attempt, maxRetries, rec.topic(), rec.partition(), rec.offset(), rec.key(), rootMessage(ex)));
        return handler;
    }

    // ===================== Container factories tipadas =====================

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, OrderEvent> orderEventsListenerFactory(
            ConsumerFactory<?, ?> baseConsumerFactory, DefaultErrorHandler kafkaErrorHandler) {
        return factory(baseConsumerFactory, kafkaErrorHandler, OrderEvent.class);
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, ShipmentEvent> shipmentEventsListenerFactory(
            ConsumerFactory<?, ?> baseConsumerFactory, DefaultErrorHandler kafkaErrorHandler) {
        return factory(baseConsumerFactory, kafkaErrorHandler, ShipmentEvent.class);
    }

    private <T> ConcurrentKafkaListenerContainerFactory<String, T> factory(
            ConsumerFactory<?, ?> base, DefaultErrorHandler errorHandler, Class<T> type) {
        Map<String, Object> props = new HashMap<>(base.getConfigurationProperties());
        DefaultKafkaConsumerFactory<String, T> consumerFactory = new DefaultKafkaConsumerFactory<>(
            props,
            new ErrorHandlingDeserializer<>(new StringDeserializer()),
            new ErrorHandlingDeserializer<>(new JsonEventDeserializer<>(type)));

        ConcurrentKafkaListenerContainerFactory<String, T> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(concurrency);
        factory.setCommonErrorHandler(errorHandler);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        return factory;
    }

    private static String rootMessage(Throwable ex) {
        Throwable root = ex;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        return root.getClass().getSimpleName() + ": " + root.getMessage();
    }
}
