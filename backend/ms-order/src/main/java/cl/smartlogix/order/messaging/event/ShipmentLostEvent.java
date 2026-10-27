package cl.smartlogix.order.messaging.event;

/** Evento entrante: el envio asociado al pedido se declaro perdido. */
public record ShipmentLostEvent(
    String eventId,
    Long orderId,
    String trackingNumber,
    String reason,
    String occurredAt
) {}
