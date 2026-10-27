package cl.smartlogix.brokeradmin.admin.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

/**
 * Solicitud de creacion de una cola. El nombre no puede ser vacio y solo admite
 * caracteres seguros para nombres de recursos AMQP.
 */
public record CreateQueueRequest(
    @NotBlank(message = "El nombre de la cola es obligatorio")
    @Size(max = 255, message = "El nombre no puede superar 255 caracteres")
    @Pattern(regexp = "^[a-zA-Z0-9._\\-]+$",
             message = "El nombre solo admite letras, numeros, punto, guion y guion bajo")
    String name,

    /** Si la cola sobrevive a reinicios del broker. Por defecto true. */
    Boolean durable
) {
    public boolean durableOrDefault() {
        return durable == null || durable;
    }
}
