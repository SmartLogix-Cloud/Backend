package cl.smartlogix.analytics.streaming;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.listener.AbstractConsumerSeekAware;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Base para consumidores cuyas metricas viven en memoria: la primera vez que esta
 * instancia recibe una particion, se posiciona al INICIO del topico (replay) para
 * reconstruir el estado a partir del historial retenido por Kafka.
 *
 * <p>Las proyecciones son idempotentes (deduplican por eventId y ordenan por
 * occurredAt), por lo que re-leer eventos ya procesados no altera las metricas.
 */
@Slf4j
public abstract class ReplayableConsumer extends AbstractConsumerSeekAware {

    private final boolean replayOnStartup;
    private final Set<TopicPartition> replayed = ConcurrentHashMap.newKeySet();

    protected ReplayableConsumer(boolean replayOnStartup) {
        this.replayOnStartup = replayOnStartup;
    }

    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        super.onPartitionsAssigned(assignments, callback);
        if (!replayOnStartup) {
            return;
        }
        assignments.keySet().stream()
            .filter(replayed::add)
            .forEach(tp -> {
                log.info("Replay de {} desde el inicio para reconstruir metricas", tp);
                callback.seekToBeginning(tp.topic(), tp.partition());
            });
    }
}
