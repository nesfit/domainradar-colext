package cz.vut.fit.domainradar.serialization;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.avro.AvroSchema;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class AvroSerde<T> implements Serde<T> {

    private final AvroSerializer<T> _serializer;
    private final AvroDeserializer<T> _deserializer;
    private final AvroSchema _schema;

    public AvroSerde(ObjectMapper objectMapper, JavaType forType) {
        _serializer = new AvroSerializer<>(objectMapper, forType);
        _deserializer = new AvroDeserializer<>(objectMapper, forType);
        _schema = AvroSchemaContainer.get(objectMapper, forType);
    }

    public static <T> AvroSerde<T> of(ObjectMapper objectMapper, TypeReference<T> typeRef) {
        return new AvroSerde<>(objectMapper, objectMapper.constructType(typeRef));
    }

    public static <T> AvroSerde<T> of(ObjectMapper objectMapper, Class<T> cls) {
        return new AvroSerde<>(objectMapper, objectMapper.constructType(cls));
    }

    public AvroSchema schema() {
        return _schema;
    }

    @Override
    public Serializer<T> serializer() {
        return _serializer;
    }

    @Override
    public Deserializer<T> deserializer() {
        return _deserializer;
    }
}
