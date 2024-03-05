package cz.vut.fit.domainradar.serialization;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.ByteBufferInputStream;

import java.io.IOException;
import java.nio.ByteBuffer;

public class JsonDeserializerTypedByTypeReference<T> implements Deserializer<T> {

    private final ObjectMapper _objectMapper;
    private final TypeReference<T> _typeRef;

    public JsonDeserializerTypedByTypeReference(ObjectMapper objectMapper, TypeReference<T> forType) {
        _objectMapper = objectMapper;
        _typeRef = forType;
    }


    @Override
    public T deserialize(String s, byte[] bytes) {
        if (bytes == null)
            return null;

        try {
            return _objectMapper.readValue(bytes, _typeRef);
        } catch (Exception e) {
            throw new SerializationException(e);
        }
    }

    @Override
    public T deserialize(String topic, Headers headers, ByteBuffer data) {
        if (data == null)
            return null;

        try (var is = new ByteBufferInputStream(data)) {
            return _objectMapper.readValue(is, _typeRef);
        } catch (IOException e) {
            throw new SerializationException(e);
        }
    }
}
