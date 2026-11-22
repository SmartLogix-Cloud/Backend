package cl.smartlogix.order.dto;

import cl.smartlogix.order.model.OrderTracking;

import java.time.OffsetDateTime;

/** Estado logistico de un pedido, proyectado desde los eventos Kafka de envio. */
public record OrderTrackingDto(
    Long orderId,
    Long shipmentId,
    String trackingNumber,
    String carrierName,
    String shipmentStatus,
    String lastLocation,
    String lastComment,
    OffsetDateTime lastEventAt,
    int eventsReceived,
    OffsetDateTime updatedAt
) {
    public static OrderTrackingDto from(OrderTracking t) {
        return new OrderTrackingDto(t.getOrderId(), t.getShipmentId(), t.getTrackingNumber(),
            t.getCarrierName(), t.getShipmentStatus(), t.getLastLocation(), t.getLastComment(),
            t.getLastEventAt(), t.getEventsReceived(), t.getUpdatedAt());
    }
}
