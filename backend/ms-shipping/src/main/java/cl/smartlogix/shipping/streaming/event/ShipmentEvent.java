package cl.smartlogix.shipping.streaming.event;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

/**
 * Evento de seguimiento de un envio (topico {@code smartlogix.shipment-events}).
 * Clave del mensaje: {@code shipmentId}; junto a cleanup.policy=compact,delete el
 * topico conserva siempre el ultimo estado de cada envio.
 *
 * @param eventId        UUID unico (idempotencia en consumidores)
 * @param eventType      SHIPMENT_CREATED / CARRIER_ASSIGNED / SHIPMENT_STATUS_CHANGED
 * @param shipmentId     id del envio (clave de particionado)
 * @param orderId        pedido asociado: ms-order actualiza su proyeccion de seguimiento
 * @param trackingNumber numero de seguimiento visible para el cliente
 * @param carrierName    transportista asignado (si existe)
 * @param previousStatus estado anterior (null en SHIPMENT_CREATED)
 * @param newStatus      estado actual del envio
 * @param location       ubicacion reportada en el hito (si existe)
 * @param comment        comentario del hito
 * @param occurredAt     instante ISO-8601 del hecho (ordenamiento y deteccion de desorden)
 * @param source         microservicio emisor
 */
public record ShipmentEvent(
    @NotBlank String eventId,
    @NotNull ShipmentEventType eventType,
    @NotNull Long shipmentId,
    @NotNull Long orderId,
    String trackingNumber,
    String carrierName,
    String previousStatus,
    @NotBlank String newStatus,
    String location,
    String comment,
    @NotBlank String occurredAt,
    @NotBlank String source
) {}
