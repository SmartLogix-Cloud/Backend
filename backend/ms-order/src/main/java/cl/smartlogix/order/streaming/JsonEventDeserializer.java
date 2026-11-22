package cl.smartlogix.order.streaming;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

/**
 * Deserializer Kafka tipado: convierte el JSON del mensaje al DTO del evento.
 *
 * <p>Ignora campos desconocidos para tolerar la evolucion del esquema (un productor
 * puede agregar campos sin romper a los consumidores). Un JSON invalido lanza
 * {@link SerializationException}; envuelto en {@code ErrorHandlingDeserializer},
 * el mensaje se deriva al DLT sin reintentos ni bloquear la particion.
 */
public class JsonEventDeserializer<T> implements Deserializer<T> {

    private static final ObjectMapper MAPPER = new ObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private final Class<T> type;

    public JsonEventDeserializer(Class<T> type) {
        this.type = type;
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            return MAPPER.readValue(data, type);
        } catch (IOException e) {
            throw new SerializationException(
                "Payload invalido en " + topic + " para " + type.getSimpleName(), e);
        }
    }
}
