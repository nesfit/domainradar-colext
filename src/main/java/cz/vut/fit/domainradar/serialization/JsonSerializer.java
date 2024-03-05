package cz.vut.fit.domainradar.serialization;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

public class JsonSerializer<T> implements Serializer<T> {

    private final ObjectMapper _objectMapper;

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
