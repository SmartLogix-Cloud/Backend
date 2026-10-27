package cl.smartlogix.order.messaging.event;

/** Evento: el pedido esta preparado y listo para generar su envio. */
public record OrderReadyForShipmentEvent(
    String eventId,
    Long orderId,
    String orderCode,
    String occurredAt
) {}
