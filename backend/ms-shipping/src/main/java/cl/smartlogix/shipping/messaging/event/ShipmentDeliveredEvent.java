package cl.smartlogix.shipping.messaging.event;

/** Evento: el envio fue entregado. */
public record ShipmentDeliveredEvent(
    String eventId,
    Long orderId,
    String trackingNumber,
    String occurredAt
) {}
