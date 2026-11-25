package cl.smartlogix.order.streaming;

import cl.smartlogix.order.streaming.event.ShipmentEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
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
 * Infraestructura de consumo Kafka de ms-order.
 *
 * <p>Estrategia de errores (los mensajes defectuosos nunca bloquean la particion):
 * <ol>
 *   <li><b>Payload ilegible</b>: {@link ErrorHandlingDeserializer} captura el error de
 *       deserializacion y el mensaje va directo al DLT (no reintentable).</li>
 *   <li><b>Evento invalido</b> ({@link InvalidEventException}): directo al DLT.</li>
 *   <li><b>Fallo transitorio</b> (BD caida, timeout): reintentos con backoff
 *       exponencial; si se agotan, el mensaje va al DLT {@code <topico>.DLT}
 *       conservando la particion y headers con la causa del error.</li>
 * </ol>
 */
@Configuration
@Slf4j
public class KafkaConsumerConfig {

    @Bean
    DefaultErrorHandler kafkaErrorHandler(
            KafkaOperations<?, ?> kafkaTemplate,
            @Value("${smartlogix.kafka.retry.max-retries:3}") int maxRetries,
            @Value("${smartlogix.kafka.retry.initial-interval-ms:1000}") long initialInterval,
            @Value("${smartlogix.kafka.retry.multiplier:2.0}") double multiplier) {

        DeadLetterPublishingRecoverer dltPublisher = new DeadLetterPublishingRecoverer(kafkaTemplate,
            (rec, ex) -> new TopicPartition(rec.topic() + KafkaTopicsConfig.DLT_SUFFIX, rec.partition()));

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

    /**
     * Container factory para los eventos de envio: deserializacion tipada a
     * {@link ShipmentEvent}, commit de offset por registro y {@code concurrency}
     * consumidores (uno por particion) dentro del consumer group.
     */
    @Bean
    ConcurrentKafkaListenerContainerFactory<String, ShipmentEvent> shipmentEventsListenerFactory(
            ConsumerFactory<?, ?> baseConsumerFactory,
            DefaultErrorHandler kafkaErrorHandler,
            @Value("${smartlogix.kafka.consumer.concurrency:3}") int concurrency) {

        Map<String, Object> props = new HashMap<>(baseConsumerFactory.getConfigurationProperties());
        DefaultKafkaConsumerFactory<String, ShipmentEvent> consumerFactory = new DefaultKafkaConsumerFactory<>(
            props,
            new ErrorHandlingDeserializer<>(new StringDeserializer()),
            new ErrorHandlingDeserializer<>(new JsonEventDeserializer<>(ShipmentEvent.class)));

        ConcurrentKafkaListenerContainerFactory<String, ShipmentEvent> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(concurrency);
        factory.setCommonErrorHandler(kafkaErrorHandler);
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
