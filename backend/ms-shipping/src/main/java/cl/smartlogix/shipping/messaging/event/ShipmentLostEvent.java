package cl.smartlogix.shipping.messaging.event;

/** Evento: el envio se declaro perdido. */
public record ShipmentLostEvent(
    String eventId,
    Long orderId,
    String trackingNumber,
    String reason,
    String occurredAt
) {}
