package cl.smartlogix.order.streaming;

/**
 * Evento semanticamente invalido (falla de validacion o referencia inexistente).
 * Es NO reintentable: reintentarlo nunca tendra exito, por lo que se deriva
 * directo al Dead Letter Topic.
 */
public class InvalidEventException extends RuntimeException {

    public InvalidEventException(String message) {
        super(message);
    }
}
