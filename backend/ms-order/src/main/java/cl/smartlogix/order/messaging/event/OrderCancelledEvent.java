package cl.smartlogix.order.messaging.event;

/** Evento: el pedido fue cancelado. */
public record OrderCancelledEvent(
    String eventId,
    Long orderId,
    String orderCode,
    String occurredAt
) {}
