package cl.smartlogix.order.streaming;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.stream.Collectors;

/** Valida el payload de un evento consumido contra las anotaciones de su DTO. */
@Component
@RequiredArgsConstructor
public class EventValidator {

    private final Validator validator;

    public <T> T validate(T event) {
        if (event == null) {
            throw new InvalidEventException("Evento vacio (payload nulo)");
        }
        Set<ConstraintViolation<T>> violations = validator.validate(event);
        if (!violations.isEmpty()) {
            String detail = violations.stream()
                .map(v -> v.getPropertyPath() + " " + v.getMessage())
                .sorted()
                .collect(Collectors.joining(", "));
            throw new InvalidEventException("Evento invalido: " + detail);
        }
        return event;
    }
}
