package cl.smartlogix.analytics.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Empuja las metricas a los clientes conectados por Server-Sent Events cada vez que
 * un evento Kafka las modifica (dashboard en tiempo real sin polling).
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class MetricsStreamPublisher {

    private static final long EMITTER_TIMEOUT_MS = 30L * 60 * 1000;

    private final LogisticsMetricsService metrics;
    private final List<SseEmitter> emitters = new CopyOnWriteArrayList<>();

    public SseEmitter subscribe() {
        SseEmitter emitter = new SseEmitter(EMITTER_TIMEOUT_MS);
        emitter.onCompletion(() -> emitters.remove(emitter));
        emitter.onTimeout(() -> emitters.remove(emitter));
        emitter.onError(ex -> emitters.remove(emitter));
        emitters.add(emitter);
        send(emitter, Map.of("summary", metrics.summary()));
        return emitter;
    }

    @EventListener
    public void onMetricsUpdated(MetricsUpdatedEvent update) {
        if (emitters.isEmpty()) {
            return;
        }
        Map<String, Object> payload = Map.of("event", update.cause(), "summary", metrics.summary());
        emitters.forEach(emitter -> send(emitter, payload));
    }

    private void send(SseEmitter emitter, Object payload) {
        try {
            emitter.send(SseEmitter.event().name("metrics").data(payload));
        } catch (IOException | IllegalStateException ex) {
            log.debug("Cliente SSE desconectado: {}", ex.getMessage());
            emitters.remove(emitter);
        }
    }
}
