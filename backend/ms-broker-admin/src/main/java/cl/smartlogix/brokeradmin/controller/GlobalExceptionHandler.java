package cl.smartlogix.brokeradmin.controller;

import org.springframework.amqp.AmqpException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.HashMap;
import java.util.Map;

/**
 * Traduce errores de validacion y del broker a respuestas HTTP claras.
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    /** Errores de validacion de los DTO (@NotBlank, @Pattern, etc.) -> 400. */
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<Map<String, Object>> handleValidation(MethodArgumentNotValidException ex) {
        Map<String, String> fields = new HashMap<>();
        ex.getBindingResult().getFieldErrors()
            .forEach(fe -> fields.put(fe.getField(), fe.getDefaultMessage()));
        Map<String, Object> body = new HashMap<>();
        body.put("error", "Parametros invalidos");
        body.put("fields", fields);
        return ResponseEntity.badRequest().body(body);
    }

    /** Parametros semanticamente invalidos (p. ej. tipo de exchange no soportado) -> 400. */
    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, Object>> handleIllegalArgument(IllegalArgumentException ex) {
        return ResponseEntity.badRequest().body(Map.of("error", ex.getMessage()));
    }

    /** Fallos al interactuar con el broker -> 502 Bad Gateway. */
    @ExceptionHandler(AmqpException.class)
    public ResponseEntity<Map<String, Object>> handleAmqp(AmqpException ex) {
        return ResponseEntity.status(HttpStatus.BAD_GATEWAY)
            .body(Map.of("error", "Error al operar sobre el broker", "detail", String.valueOf(ex.getMessage())));
    }
}
