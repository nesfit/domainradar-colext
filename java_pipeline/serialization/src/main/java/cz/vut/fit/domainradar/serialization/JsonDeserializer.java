package cz.vut.fit.domainradar.serialization;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.ByteBufferInputStream;

import java.nio.ByteBuffer;

/**
 * A custom JSON deserializer for Kafka that uses Jackson to deserialize JSON data into Java objects.
 *
 * @param <T> The type of the deserialized object.
 * @author Ondřej Ondryáš
 */
public class JsonDeserializer<T> implements Deserializer<T> {
    private final ObjectMapper _objectMapper;
    private final JavaType _forType;

    /**
     * Creates a new JSON deserializer for the specified non-generic class.
     *
     * @param objectMapper The ObjectMapper instance to use for deserialization.
     * @param forType      The class of the object to deserialize.
     */
    public JsonDeserializer(ObjectMapper objectMapper, Class<T> forType) {
        _objectMapper = objectMapper;
        _forType = objectMapper.constructType(forType);
    }

    /**
     * Creates a new JSON deserializer for the specified type. Used primarily for generic types.
     *
     * @param objectMapper The ObjectMapper instance to use for deserialization.
     * @param forType      The TypeReference encapsulating the generic type to deserialize.
     */
    public JsonDeserializer(ObjectMapper objectMapper, TypeReference<T> forType) {
        _objectMapper = objectMapper;
        _forType = objectMapper.constructType(forType);
    }

    @Override
    public T deserialize(String s, byte[] bytes) {
        if (bytes == null || bytes.length == 0)
            return null;

        try {
            return _objectMapper.readValue(bytes, _forType);
        } catch (Exception e) {
            throw new SerializationException(e);
        }
    }

    @Override
    public T deserialize(String topic, Headers headers, ByteBuffer data) {
        if (data == null || !data.hasRemaining())
            return null;

        try (var is = new ByteBufferInputStream(data)) {
            return _objectMapper.readValue(is, _forType);
        } catch (Exception e) {
            throw new SerializationException(e);
        }
    }
}
