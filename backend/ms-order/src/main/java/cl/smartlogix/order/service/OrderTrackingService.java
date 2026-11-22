package cl.smartlogix.order.service;

import cl.smartlogix.order.dto.OrderTrackingDto;
import cl.smartlogix.order.model.OrderTracking;
import cl.smartlogix.order.repository.OrderRepository;
import cl.smartlogix.order.repository.OrderTrackingRepository;
import cl.smartlogix.order.streaming.InvalidEventException;
import cl.smartlogix.order.streaming.event.ShipmentEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;

/**
 * Logica de dominio del seguimiento logistico de pedidos.
 * Mantiene la proyeccion {@link OrderTracking} a partir de eventos de envio.
 */
@Service
@Slf4j
@RequiredArgsConstructor
@Transactional
public class OrderTrackingService {

    private final OrderTrackingRepository trackingRepository;
    private final OrderRepository orderRepository;

    /**
     * Aplica un evento de envio a la proyeccion del pedido.
     * <ul>
     *   <li>Idempotente: un evento ya aplicado (mismo eventId) se ignora.</li>
     *   <li>Tolerante al desorden: un evento mas antiguo que el ultimo aplicado no
     *       sobreescribe el estado vigente.</li>
     *   <li>Pedido inexistente: {@link InvalidEventException} (va al DLT sin reintentos).</li>
     * </ul>
     */
    public void applyShipmentEvent(ShipmentEvent event) {
        if (!orderRepository.existsById(event.orderId())) {
            throw new InvalidEventException("El pedido " + event.orderId() + " no existe en ms-order");
        }
        OffsetDateTime occurredAt = parseInstant(event.occurredAt());

        OrderTracking tracking = trackingRepository.findById(event.orderId())
            .orElseGet(() -> OrderTracking.builder().orderId(event.orderId()).build());

        if (event.eventId().equals(tracking.getLastEventId())) {
            log.info("Evento {} ya aplicado al pedido {} (duplicado ignorado)", event.eventId(), event.orderId());
            return;
        }
        tracking.setEventsReceived(tracking.getEventsReceived() + 1);
        if (tracking.getLastEventAt() != null && occurredAt.isBefore(tracking.getLastEventAt())) {
            log.warn("Evento {} fuera de orden para pedido {} ({} < {}): no altera el estado vigente",
                event.eventId(), event.orderId(), occurredAt, tracking.getLastEventAt());
            trackingRepository.save(tracking);
            return;
        }

        tracking.setShipmentId(event.shipmentId());
        tracking.setShipmentStatus(event.newStatus());
        if (event.trackingNumber() != null) {
            tracking.setTrackingNumber(event.trackingNumber());
        }
        if (event.carrierName() != null) {
            tracking.setCarrierName(event.carrierName());
        }
        if (event.location() != null) {
            tracking.setLastLocation(event.location());
        }
        tracking.setLastComment(event.comment());
        tracking.setLastEventId(event.eventId());
        tracking.setLastEventAt(occurredAt);
        trackingRepository.save(tracking);
    }

    @Transactional(readOnly = true)
    public OrderTrackingDto findByOrder(Long orderId) {
        return trackingRepository.findById(orderId)
            .map(OrderTrackingDto::from)
            .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND,
                "Sin informacion de seguimiento para el pedido " + orderId));
    }

    private static OffsetDateTime parseInstant(String value) {
        try {
            return OffsetDateTime.parse(value);
        } catch (DateTimeParseException e) {
            throw new InvalidEventException("occurredAt no es una fecha ISO-8601 valida: " + value);
        }
    }
}
