package cl.smartlogix.order.service;

import cl.smartlogix.order.model.OrderTracking;
import cl.smartlogix.order.repository.OrderRepository;
import cl.smartlogix.order.repository.OrderTrackingRepository;
import cl.smartlogix.order.streaming.InvalidEventException;
import cl.smartlogix.order.streaming.event.ShipmentEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.OffsetDateTime;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class OrderTrackingServiceTest {

    private static final OffsetDateTime T0 = OffsetDateTime.parse("2026-11-21T10:00:00Z");

    private OrderTrackingRepository trackingRepository;
    private OrderRepository orderRepository;
    private OrderTrackingService service;

    @BeforeEach
    void setUp() {
        trackingRepository = mock(OrderTrackingRepository.class);
        orderRepository = mock(OrderRepository.class);
        service = new OrderTrackingService(trackingRepository, orderRepository);
    }

    @Test
    void createsProjectionOnFirstEvent() {
        when(orderRepository.existsById(5L)).thenReturn(true);
        when(trackingRepository.findById(5L)).thenReturn(Optional.empty());

        service.applyShipmentEvent(event("e1", "EN_TRANSITO", T0));

        ArgumentCaptor<OrderTracking> captor = ArgumentCaptor.forClass(OrderTracking.class);
        verify(trackingRepository).save(captor.capture());
        OrderTracking saved = captor.getValue();
        assertThat(saved.getOrderId()).isEqualTo(5L);
        assertThat(saved.getShipmentStatus()).isEqualTo("EN_TRANSITO");
        assertThat(saved.getLastEventId()).isEqualTo("e1");
        assertThat(saved.getEventsReceived()).isEqualTo(1);
    }

    @Test
    void ignoresDuplicateEvent() {
        when(orderRepository.existsById(5L)).thenReturn(true);
        when(trackingRepository.findById(5L)).thenReturn(Optional.of(existing("e1", T0)));

        service.applyShipmentEvent(event("e1", "EN_TRANSITO", T0));

        verify(trackingRepository, never()).save(any());
    }

    @Test
    void olderEventDoesNotOverrideStatus() {
        OrderTracking current = existing("e2", T0.plusMinutes(10));
        when(orderRepository.existsById(5L)).thenReturn(true);
        when(trackingRepository.findById(5L)).thenReturn(Optional.of(current));

        service.applyShipmentEvent(event("e1", "RETIRADO", T0));

        assertThat(current.getShipmentStatus()).isEqualTo("EN_TRANSITO");
        assertThat(current.getEventsReceived()).isEqualTo(2);
    }

    @Test
    void unknownOrderIsInvalidEvent() {
        when(orderRepository.existsById(5L)).thenReturn(false);

        assertThatThrownBy(() -> service.applyShipmentEvent(event("e1", "EN_TRANSITO", T0)))
            .isInstanceOf(InvalidEventException.class);
    }

    private static ShipmentEvent event(String id, String status, OffsetDateTime at) {
        return new ShipmentEvent(id, "SHIPMENT_STATUS_CHANGED", 9L, 5L, "TRK-9", "Chilexpress",
            null, status, "Santiago", null, at.toString(), "ms-shipping");
    }

    private static OrderTracking existing(String lastEventId, OffsetDateTime at) {
        return OrderTracking.builder().orderId(5L).shipmentId(9L).shipmentStatus("EN_TRANSITO")
            .lastEventId(lastEventId).lastEventAt(at).eventsReceived(1).build();
    }
}
