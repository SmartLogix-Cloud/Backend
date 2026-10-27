package cl.smartlogix.brokeradmin.admin.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

/**
 * Solicitud de creacion (o eliminacion) de un binding cola-exchange.
 * La routingKey es opcional (los exchanges fanout la ignoran).
 */
public record CreateBindingRequest(
    @NotBlank(message = "La cola destino es obligatoria")
    @Size(max = 255)
    String queue,

    @NotBlank(message = "El exchange origen es obligatorio")
    @Size(max = 255)
    String exchange,

    @Size(max = 255, message = "La routingKey no puede superar 255 caracteres")
    String routingKey
) {
    public String routingKeyOrEmpty() {
        return routingKey == null ? "" : routingKey;
    }
}
