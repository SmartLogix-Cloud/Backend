package cl.smartlogix.brokeradmin.kafka.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PositiveOrZero;

/**
 * Reposicionamiento de los offsets de un consumer group sobre un topico
 * (p.ej. para re-procesar eventos). El grupo debe estar inactivo (EMPTY).
 *
 * @param topic    topico cuyos offsets se reposicionan (todas sus particiones)
 * @param strategy EARLIEST (inicio), LATEST (final) o TO_OFFSET (offset exacto)
 * @param offset   offset destino, solo para TO_OFFSET
 */
public record ResetOffsetsRequest(
    @NotBlank(message = "topic es obligatorio") String topic,
    @NotNull(message = "strategy es obligatorio") Strategy strategy,
    @PositiveOrZero Long offset
) {
    public enum Strategy { EARLIEST, LATEST, TO_OFFSET }
}
