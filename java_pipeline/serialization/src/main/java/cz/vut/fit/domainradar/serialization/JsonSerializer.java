package cz.vut.fit.domainradar.serialization;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

/**
 * A custom JSON serializer for Kafka that uses Jackson to serialize JSON data into Java objects.
 *
 * @param <T> The type of the serialized object.
 * @author Ondřej Ondryáš
 */
public class JsonSerializer<T> implements Serializer<T> {
    protected final ObjectMapper _objectMapper;

    public JsonSerializer(ObjectMapper objectMapper) {
        _objectMapper = objectMapper;
    }

    @Override
    public byte[] serialize(String topic, T object) {
        if (object == null)
            return null;

        try {
            return _objectMapper.writeValueAsBytes(object);
        } catch (JsonProcessingException e) {
            throw new SerializationException(e);
        }
    }
}
