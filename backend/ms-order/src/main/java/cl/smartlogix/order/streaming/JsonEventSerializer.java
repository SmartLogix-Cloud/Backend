package cl.smartlogix.order.streaming;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Serializer Kafka de eventos de dominio (records DTO) a JSON UTF-8.
 *
 * <p>Si el valor ya es {@code byte[]} se envia tal cual: es el caso del
 * {@code DeadLetterPublishingRecoverer} al reenviar al DLT un payload que no se
 * pudo deserializar (se conservan los bytes originales para su analisis).
 */
public class JsonEventSerializer implements Serializer<Object> {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public byte[] serialize(String topic, Object data) {
        if (data == null) {
            return null;
        }
        if (data instanceof byte[] raw) {
            return raw;
        }
        try {
            return MAPPER.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            throw new SerializationException("No se pudo serializar el evento para el topico " + topic, e);
        }
    }
}
