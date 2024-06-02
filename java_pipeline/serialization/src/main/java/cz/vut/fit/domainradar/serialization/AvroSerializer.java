package cz.vut.fit.domainradar.serialization;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.avro.AvroSchema;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

public class AvroSerializer<T> implements Serializer<T> {
    private final ObjectMapper _objectMapper;
    private final AvroSchema _schema;

    public AvroSerializer(ObjectMapper objectMapper, Class<T> forType) {
        this(objectMapper, objectMapper.constructType(forType));
    }

    public AvroSerializer(ObjectMapper objectMapper, TypeReference<T> forType) {
        this(objectMapper, objectMapper.constructType(forType));
    }

    public AvroSerializer(ObjectMapper objectMapper, JavaType forType) {
        _objectMapper = objectMapper;
        _schema = AvroSchemaContainer.get(objectMapper, forType);
    }

    @Override
    public byte[] serialize(String topic, T object) {
        if (object == null)
            return null;

        try {
            return _objectMapper
                    .writer(_schema)
                    .writeValueAsBytes(object);
        } catch (JsonProcessingException e) {
            throw new SerializationException(e);
        }
    }
}

