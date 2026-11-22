package cl.smartlogix.brokeradmin.kafka.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

import java.util.Map;

/**
 * Solicitud de creacion de un topico Kafka.
 *
 * @param name              nombre (letras, numeros, punto, guion y guion bajo)
 * @param partitions        numero de particiones (paralelismo maximo por consumer group)
 * @param replicationFactor copias de cada particion (maximo = numero de brokers)
 * @param configs           configuraciones del topico (retention.ms, cleanup.policy,
 *                          min.insync.replicas, ...). Opcional.
 */
public record CreateTopicRequest(
    @NotBlank(message = "El nombre del topico es obligatorio")
    @Size(max = 249, message = "El nombre no puede superar 249 caracteres")
    @Pattern(regexp = "^[a-zA-Z0-9._-]+$",
             message = "El nombre solo admite letras, numeros, punto, guion y guion bajo")
    String name,

    @NotNull(message = "partitions es obligatorio")
    @Min(value = 1, message = "Debe tener al menos 1 particion")
    @Max(value = 100, message = "Maximo 100 particiones")
    Integer partitions,

    @NotNull(message = "replicationFactor es obligatorio")
    @Min(value = 1, message = "replicationFactor minimo 1")
    @Max(value = 3, message = "replicationFactor no puede superar el numero de brokers (3)")
    Short replicationFactor,

    Map<String, String> configs
) {}
