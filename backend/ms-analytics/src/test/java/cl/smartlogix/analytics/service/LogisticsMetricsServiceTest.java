package cl.smartlogix.analytics.service;

import cl.smartlogix.analytics.dto.MetricsSummary;
import cl.smartlogix.analytics.streaming.InvalidEventException;
import cl.smartlogix.analytics.streaming.event.OrderEvent;
import cl.smartlogix.analytics.streaming.event.ShipmentEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationEventPublisher;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class LogisticsMetricsServiceTest {

    private static final OffsetDateTime T0 = OffsetDateTime.parse("2026-11-21T10:00:00Z");

    private ApplicationEventPublisher publisher;
    private LogisticsMetricsService service;

    @BeforeEach
    void setUp() {
        publisher = mock(ApplicationEventPublisher.class);
        service = new LogisticsMetricsService(publisher, 50);
    }

    @Test
    void aggregatesOrdersByStatusAndRevenue() {
        service.applyOrderEvent(order("e1", 1L, null, "PENDIENTE_PREPARACION", "1000", T0), 0, 0);
        service.applyOrderEvent(order("e2", 2L, null, "PENDIENTE_PREPARACION", "500", T0), 1, 0);
        service.applyOrderEvent(order("e3", 2L, "PENDIENTE_PREPARACION", "CANCELADO", "500", T0.plusMinutes(1)), 1, 1);

        MetricsSummary s = service.summary();

        assertThat(s.totalOrders()).isEqualTo(2);
        assertThat(s.ordersByStatus()).containsEntry("PENDIENTE_PREPARACION", 1L).containsEntry("CANCELADO", 1L);
        assertThat(s.activeRevenue()).isEqualByComparingTo("1000"); // el cancelado no suma
        assertThat(s.eventsProcessed()).isEqualTo(3);
        verify(publisher, times(3)).publishEvent(any(MetricsUpdatedEvent.class));
    }

    @Test
    void duplicateEventIsIgnored() {
        OrderEvent e = order("dup", 1L, null, "PENDIENTE_PREPARACION", "100", T0);

        assertThat(service.applyOrderEvent(e, 0, 0)).isTrue();
        assertThat(service.applyOrderEvent(e, 0, 0)).isFalse();

        assertThat(service.summary().eventsProcessed()).isEqualTo(1);
        assertThat(service.summary().duplicatesIgnored()).isEqualTo(1);
    }

    @Test
    void outOfOrderEventDoesNotOverrideCurrentState() {
        service.applyShipmentEvent(shipment("s2", 7L, "EN_TRANSITO", T0.plusMinutes(5)), 2, 10);
        boolean applied = service.applyShipmentEvent(shipment("s1", 7L, "RETIRADO", T0), 2, 9);

        assertThat(applied).isFalse();
        assertThat(service.summary().shipmentsByStatus()).containsEntry("EN_TRANSITO", 1L);
    }

    @Test
    void computesIncidentsAndDeliveryRate() {
        service.applyShipmentEvent(shipment("a", 1L, "ENTREGADO", T0), 0, 0);
        service.applyShipmentEvent(shipment("b", 2L, "PERDIDO", T0), 1, 0);
        service.applyShipmentEvent(shipment("c", 3L, "EN_TRANSITO", T0), 2, 0);
        service.applyShipmentEvent(shipment("d", 4L, "ENTREGADO", T0), 0, 1);

        MetricsSummary s = service.summary();

        assertThat(s.totalShipments()).isEqualTo(4);
        assertThat(s.activeIncidents()).isEqualTo(1);
        assertThat(s.deliveryRate()).isEqualTo(50.0);
        assertThat(service.activeIncidents()).hasSize(1);
        assertThat(service.recentEvents(2)).hasSize(2);
    }

    @Test
    void invalidTimestampIsRejected() {
        ShipmentEvent bad = new ShipmentEvent("x", "SHIPMENT_STATUS_CHANGED", 1L, 1L, null, null,
            null, "EN_TRANSITO", null, null, "ayer", "test");

        assertThatThrownBy(() -> service.applyShipmentEvent(bad, 0, 0))
            .isInstanceOf(InvalidEventException.class);
    }

    private static OrderEvent order(String id, Long orderId, String prev, String next, String total, OffsetDateTime at) {
        return new OrderEvent(id, prev == null ? "ORDER_CREATED" : "ORDER_STATUS_CHANGED", orderId,
            "PED-" + orderId, "cli-1", "STANDARD", prev, next, new BigDecimal(total), 1, null, at.toString(), "test");
    }

    private static ShipmentEvent shipment(String id, Long shipmentId, String status, OffsetDateTime at) {
        return new ShipmentEvent(id, "SHIPMENT_STATUS_CHANGED", shipmentId, 100L + shipmentId, "TRK-" + shipmentId,
            "Carrier", null, status, null, null, at.toString(), "test");
    }
}
