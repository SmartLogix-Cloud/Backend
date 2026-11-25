package cl.smartlogix.analytics.streaming.event;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PositiveOrZero;

import java.math.BigDecimal;

/**
 * Evento del ciclo de vida de un pedido (topico {@code smartlogix.order-events}),
 * producido por ms-order. Tipos y estados como texto: ms-analytics no se acopla a
 * los enums del productor y tolera valores nuevos.
 */
public record OrderEvent(
    @NotBlank String eventId,
    @NotBlank String eventType,
    @NotNull Long orderId,
    @NotBlank String orderCode,
    String customerId,
    String orderType,
    String previousStatus,
    @NotBlank String newStatus,
    @PositiveOrZero BigDecimal total,
    @PositiveOrZero Integer itemCount,
    String reason,
    @NotBlank String occurredAt,
    String source
) {}
