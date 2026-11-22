package cl.smartlogix.brokeradmin.kafka;

import lombok.Getter;
import org.springframework.http.HttpStatus;

/** Error de una operacion de administracion de Kafka, con el estado HTTP que le corresponde. */
@Getter
public class KafkaAdminException extends RuntimeException {

    private final HttpStatus status;

    public KafkaAdminException(HttpStatus status, String message, Throwable cause) {
        super(message, cause);
        this.status = status;
    }

    public KafkaAdminException(HttpStatus status, String message) {
        this(status, message, null);
    }
}
