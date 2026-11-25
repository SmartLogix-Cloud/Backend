package cl.smartlogix.analytics.controller;

import cl.smartlogix.analytics.dto.MetricsSummary;
import cl.smartlogix.analytics.dto.RecentEvent;
import cl.smartlogix.analytics.service.LogisticsMetricsService;
import cl.smartlogix.analytics.service.MetricsStreamPublisher;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.List;
import java.util.Map;

/** API de analitica logistica en tiempo real (alimentada por Kafka). */
@RestController
@RequestMapping("/analytics")
@RequiredArgsConstructor
@Tag(name = "Analitica", description = "Metricas logisticas calculadas en tiempo real desde Kafka")
public class AnalyticsController {

    private final LogisticsMetricsService metrics;
    private final MetricsStreamPublisher stream;

    @Operation(summary = "Resumen de metricas",
        description = "Pedidos y envios por estado, ventas activas, incidencias y tasa de entrega.")
    @GetMapping("/summary")
    public MetricsSummary summary() {
        return metrics.summary();
    }

    @Operation(summary = "Ultimos eventos procesados", description = "Feed de eventos Kafka recien aplicados.")
    @GetMapping("/events")
    public List<RecentEvent> recentEvents(@RequestParam(defaultValue = "20") int limit) {
        return metrics.recentEvents(Math.min(limit, 50));
    }

    @Operation(summary = "Incidencias activas", description = "Envios perdidos, retrasados o con entrega fallida.")
    @GetMapping("/incidents")
    public List<Map<String, Object>> incidents() {
        return metrics.activeIncidents();
    }

    @Operation(summary = "Stream en tiempo real (SSE)",
        description = "Server-Sent Events: emite las metricas cada vez que llega un evento Kafka.")
    @GetMapping(path = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter stream() {
        return stream.subscribe();
    }
}
