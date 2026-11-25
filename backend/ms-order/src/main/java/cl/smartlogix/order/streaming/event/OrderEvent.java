package cl.smartlogix.order.streaming.event;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.math.BigDecimal;

/**
 * Evento del ciclo de vida de un pedido (topico {@code smartlogix.order-events}).
 * Clave del mensaje: {@code orderId} (orden garantizado por pedido).
 *
 * @param eventId        UUID unico: permite deduplicar (idempotencia en consumidores)
 * @param eventType      ORDER_CREATED u ORDER_STATUS_CHANGED
 * @param orderId        id del pedido (clave de particionado)
 * @param orderCode      codigo legible del pedido (PED-yyyyMMdd-XXXXXX)
 * @param customerId     cliente: permite metricas por cliente
 * @param orderType      STANDARD / EXPRESS: permite metricas por tipo
 * @param previousStatus estado anterior (null en ORDER_CREATED)
 * @param newStatus      estado actual del pedido
 * @param total          monto total: alimenta las metricas de ventas
 * @param itemCount      cantidad de items del pedido
 * @param reason         motivo de la transicion (si aplica)
 * @param occurredAt     instante ISO-8601 en que ocurrio el hecho
 * @param source         microservicio emisor (trazabilidad)
 */
public record OrderEvent(
    @NotBlank String eventId,
    @NotNull OrderEventType eventType,
    @NotNull Long orderId,
    @NotBlank String orderCode,
    String customerId,
    String orderType,
    String previousStatus,
    @NotBlank String newStatus,
    BigDecimal total,
    Integer itemCount,
    String reason,
    @NotBlank String occurredAt,
    @NotBlank String source
) {}
