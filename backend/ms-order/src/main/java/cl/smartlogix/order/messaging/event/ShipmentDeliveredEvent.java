package cl.smartlogix.order.messaging.event;

/** Evento entrante: el envio asociado al pedido fue entregado. */
public record ShipmentDeliveredEvent(
    String eventId,
    Long orderId,
    String trackingNumber,
    String occurredAt
) {}
