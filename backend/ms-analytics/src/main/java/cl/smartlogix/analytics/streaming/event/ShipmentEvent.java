package cl.smartlogix.analytics.streaming.event;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

/**
 * Evento de seguimiento de un envio (topico {@code smartlogix.shipment-events}),
 * producido por ms-shipping.
 */
public record ShipmentEvent(
    @NotBlank String eventId,
    @NotBlank String eventType,
    @NotNull Long shipmentId,
    @NotNull Long orderId,
    String trackingNumber,
    String carrierName,
    String previousStatus,
    @NotBlank String newStatus,
    String location,
    String comment,
    @NotBlank String occurredAt,
    String source
) {}
