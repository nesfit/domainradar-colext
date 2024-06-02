package cz.vut.fit.domainradar.serialization;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.avro.AvroSchema;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.ByteBufferInputStream;

import java.nio.ByteBuffer;

public class AvroDeserializer<T> implements Deserializer<T> {
    private final ObjectMapper _objectMapper;
    private final AvroSchema _schema;
    private final JavaType _forType;

    public AvroDeserializer(ObjectMapper objectMapper, Class<T> forType) {
        this(objectMapper, objectMapper.constructType(forType));
    }

    public AvroDeserializer(ObjectMapper objectMapper, TypeReference<T> forType) {
        this(objectMapper, objectMapper.constructType(forType));
    }

    public AvroDeserializer(ObjectMapper objectMapper, JavaType forType) {
        _objectMapper = objectMapper;
        _schema = AvroSchemaContainer.get(objectMapper, forType);
        _forType = objectMapper.constructType(forType);
    }

    @Override
    public T deserialize(String s, byte[] bytes) {
        if (bytes == null || bytes.length == 0)
            return null;

        try {
            return _objectMapper
                    .readerFor(_forType)
                    .with(_schema)
                    .readValue(bytes);
        } catch (Exception e) {
            throw new SerializationException(e);
        }
    }

    @Override
    public T deserialize(String topic, Headers headers, ByteBuffer data) {
        if (data == null || !data.hasRemaining())
            return null;

        try (var is = new ByteBufferInputStream(data)) {
            return _objectMapper
                    .readerFor(_forType)
                    .with(_schema)
                    .readValue(is);
        } catch (Exception e) {
            throw new SerializationException(e);
        }
    }
}