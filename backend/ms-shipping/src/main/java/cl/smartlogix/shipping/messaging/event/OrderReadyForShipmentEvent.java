package cl.smartlogix.shipping.messaging.event;

/** Evento entrante: un pedido esta listo para generar su envio. */
public record OrderReadyForShipmentEvent(
    String eventId,
    Long orderId,
    String orderCode,
    String occurredAt
) {}
