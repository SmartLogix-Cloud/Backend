package cl.smartlogix.analytics.service;

import cl.smartlogix.analytics.dto.MetricsSummary;
import cl.smartlogix.analytics.dto.RecentEvent;
import cl.smartlogix.analytics.streaming.InvalidEventException;
import cl.smartlogix.analytics.streaming.event.OrderEvent;
import cl.smartlogix.analytics.streaming.event.ShipmentEvent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Logica de dominio de la analitica logistica: mantiene en memoria una proyeccion
 * del estado actual de cada pedido y envio a partir de los eventos Kafka, y calcula
 * las metricas operacionales a demanda.
 *
 * <p>Garantias de la proyeccion:
 * <ul>
 *   <li><b>Idempotente</b>: cada eventId se aplica una sola vez (replays y
 *       redeliveries no duplican metricas).</li>
 *   <li><b>Tolerante al desorden</b>: un evento mas antiguo que el ultimo aplicado
 *       para la misma entidad no sobreescribe su estado.</li>
 *   <li><b>Thread-safe</b>: la invocan 3 hilos consumidores por topico.</li>
 * </ul>
 */
@Service
public class LogisticsMetricsService {

    static final Set<String> EXCLUDED_REVENUE_STATUSES = Set.of("CANCELADO", "RECHAZADO");
    static final Set<String> INCIDENT_STATUSES = Set.of("PERDIDO", "RETRASADO", "ENTREGA_FALLIDA");
    static final String DELIVERED_STATUS = "ENTREGADO";

    record OrderSnapshot(String status, String type, BigDecimal total, OffsetDateTime lastEventAt) {}

    record ShipmentSnapshot(Long orderId, String status, OffsetDateTime lastEventAt) {}

    private final Map<Long, OrderSnapshot> orders = new ConcurrentHashMap<>();
    private final Map<Long, ShipmentSnapshot> shipments = new ConcurrentHashMap<>();
    private final Set<String> processedEventIds = ConcurrentHashMap.newKeySet();
    private final AtomicLong duplicatesIgnored = new AtomicLong();
    private final AtomicReference<OffsetDateTime> lastEventAt = new AtomicReference<>();
    private final Deque<RecentEvent> recentEvents = new ArrayDeque<>();
    private final int recentEventsSize;
    private final ApplicationEventPublisher publisher;

    public LogisticsMetricsService(ApplicationEventPublisher publisher,
                                   @Value("${smartlogix.analytics.recent-events-size:50}") int recentEventsSize) {
        this.publisher = publisher;
        this.recentEventsSize = recentEventsSize;
    }

    /** @return true si el evento modifico la proyeccion; false si era duplicado o antiguo. */
    public boolean applyOrderEvent(OrderEvent e, int partition, long offset) {
        OffsetDateTime at = parse(e.occurredAt());
        if (!processedEventIds.add(e.eventId())) {
            duplicatesIgnored.incrementAndGet();
            return false;
        }
        boolean[] applied = {false};
        orders.compute(e.orderId(), (id, current) -> {
            if (current != null && at.isBefore(current.lastEventAt())) {
                return current; // evento fuera de orden: no pisa el estado vigente
            }
            applied[0] = true;
            String type = e.orderType() != null ? e.orderType() : current != null ? current.type() : "DESCONOCIDO";
            BigDecimal total = e.total() != null ? e.total() : current != null ? current.total() : BigDecimal.ZERO;
            return new OrderSnapshot(e.newStatus(), type, total, at);
        });
        if (!applied[0]) {
            duplicatesIgnored.incrementAndGet();
            return false;
        }
        record(new RecentEvent("order-events", partition, offset, e.eventType(), e.orderId(), e.newStatus(),
            "Pedido " + e.orderCode() + transition(e.previousStatus(), e.newStatus()), e.occurredAt()), at);
        return true;
    }

    /** @return true si el evento modifico la proyeccion; false si era duplicado o antiguo. */
    public boolean applyShipmentEvent(ShipmentEvent e, int partition, long offset) {
        OffsetDateTime at = parse(e.occurredAt());
        if (!processedEventIds.add(e.eventId())) {
            duplicatesIgnored.incrementAndGet();
            return false;
        }
        boolean[] applied = {false};
        shipments.compute(e.shipmentId(), (id, current) -> {
            if (current != null && at.isBefore(current.lastEventAt())) {
                return current;
            }
            applied[0] = true;
            return new ShipmentSnapshot(e.orderId(), e.newStatus(), at);
        });
        if (!applied[0]) {
            duplicatesIgnored.incrementAndGet();
            return false;
        }
        String tracking = e.trackingNumber() != null ? " (" + e.trackingNumber() + ")" : "";
        record(new RecentEvent("shipment-events", partition, offset, e.eventType(), e.shipmentId(), e.newStatus(),
            "Envio " + e.shipmentId() + tracking + transition(e.previousStatus(), e.newStatus()), e.occurredAt()), at);
        return true;
    }

    public MetricsSummary summary() {
        Collection<OrderSnapshot> orderValues = orders.values();
        Collection<ShipmentSnapshot> shipmentValues = shipments.values();

        BigDecimal revenue = orderValues.stream()
            .filter(o -> !EXCLUDED_REVENUE_STATUSES.contains(o.status()))
            .map(OrderSnapshot::total)
            .reduce(BigDecimal.ZERO, BigDecimal::add);
        long incidents = shipmentValues.stream().filter(s -> INCIDENT_STATUSES.contains(s.status())).count();
        long delivered = shipmentValues.stream().filter(s -> DELIVERED_STATUS.equals(s.status())).count();
        double deliveryRate = shipmentValues.isEmpty() ? 0.0
            : Math.round(delivered * 10000.0 / shipmentValues.size()) / 100.0;
        OffsetDateTime last = lastEventAt.get();

        return new MetricsSummary(
            orderValues.size(),
            countBy(orderValues, OrderSnapshot::status),
            countBy(orderValues, OrderSnapshot::type),
            revenue,
            shipmentValues.size(),
            countBy(shipmentValues, ShipmentSnapshot::status),
            incidents,
            deliveryRate,
            processedEventIds.size(),
            duplicatesIgnored.get(),
            last != null ? last.toString() : null,
            OffsetDateTime.now().toString());
    }

    /** Ultimos eventos aplicados, del mas reciente al mas antiguo. */
    public List<RecentEvent> recentEvents(int limit) {
        synchronized (recentEvents) {
            return recentEvents.stream().limit(Math.max(0, limit)).toList();
        }
    }

    /** Envios en estado de incidencia (perdido, retrasado, entrega fallida). */
    public List<Map<String, Object>> activeIncidents() {
        List<Map<String, Object>> result = new ArrayList<>();
        shipments.forEach((id, s) -> {
            if (INCIDENT_STATUSES.contains(s.status())) {
                result.add(Map.of("shipmentId", id, "orderId", s.orderId(), "status", s.status(),
                    "since", s.lastEventAt().toString()));
            }
        });
        return result;
    }

    private void record(RecentEvent event, OffsetDateTime at) {
        lastEventAt.accumulateAndGet(at, (prev, next) -> prev == null || next.isAfter(prev) ? next : prev);
        synchronized (recentEvents) {
            recentEvents.addFirst(event);
            while (recentEvents.size() > recentEventsSize) {
                recentEvents.removeLast();
            }
        }
        publisher.publishEvent(new MetricsUpdatedEvent(event));
    }

    private static <T> Map<String, Long> countBy(Collection<T> values, Function<T, String> key) {
        return values.stream().collect(Collectors.groupingBy(key, TreeMap::new, Collectors.counting()));
    }

    private static String transition(String previous, String next) {
        return previous != null ? ": " + previous + " -> " + next : ": " + next;
    }

    private static OffsetDateTime parse(String value) {
        try {
            return OffsetDateTime.parse(value);
        } catch (DateTimeParseException | NullPointerException ex) {
            throw new InvalidEventException("occurredAt no es una fecha ISO-8601 valida: " + value);
        }
    }
}
