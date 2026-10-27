package cl.smartlogix.brokeradmin.admin.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

/**
 * Solicitud de creacion de un exchange. El tipo debe ser uno de los soportados
 * por AMQP (direct, topic, fanout, headers).
 */
public record CreateExchangeRequest(
    @NotBlank(message = "El nombre del exchange es obligatorio")
    @Size(max = 255, message = "El nombre no puede superar 255 caracteres")
    @Pattern(regexp = "^[a-zA-Z0-9._\\-]+$",
             message = "El nombre solo admite letras, numeros, punto, guion y guion bajo")
    String name,

    @NotBlank(message = "El tipo del exchange es obligatorio")
    @Pattern(regexp = "^(direct|topic|fanout|headers)$",
             message = "El tipo debe ser direct, topic, fanout o headers")
    String type,

    /** Si el exchange sobrevive a reinicios del broker. Por defecto true. */
    Boolean durable
) {
    public boolean durableOrDefault() {
        return durable == null || durable;
    }
}
