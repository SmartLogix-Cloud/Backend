package cl.smartlogix.order.streaming.event;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

/**
 * Evento de seguimiento de un envio, producido por ms-shipping
 * (topico {@code smartlogix.shipment-events}, clave = {@code shipmentId}).
 * Los estados viajan como texto para no acoplar ms-order al enum de ms-shipping.
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
