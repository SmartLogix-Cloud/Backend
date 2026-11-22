package cl.smartlogix.brokeradmin.kafka.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

/**
 * Aumento del numero de particiones de un topico (Kafka no permite reducirlas).
 *
 * @param totalPartitions nuevo total de particiones (debe ser mayor al actual)
 */
public record IncreasePartitionsRequest(
    @NotNull(message = "totalPartitions es obligatorio")
    @Min(value = 1, message = "totalPartitions minimo 1")
    @Max(value = 100, message = "Maximo 100 particiones")
    Integer totalPartitions
) {}
