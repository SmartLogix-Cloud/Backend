package cl.smartlogix.shipping.messaging.event;

/** Evento entrante: un pedido fue cancelado. */
public record OrderCancelledEvent(
    String eventId,
    Long orderId,
    String orderCode,
    String occurredAt
) {}
