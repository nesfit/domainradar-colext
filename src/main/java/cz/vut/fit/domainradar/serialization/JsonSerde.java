package cz.vut.fit.domainradar.serialization;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class JsonSerde<T> implements Serde<T> {

    private final JsonSerializer<T> _serializer;
    private final Deserializer<T> _deserializer;

    public JsonSerde(ObjectMapper objectMapper, TypeReference<T> typeRef) {
        _serializer = new JsonSerializer<>(objectMapper);
        _deserializer = new JsonDeserializerTypedByTypeReference<>(objectMapper, typeRef);
    }

    public JsonSerde(ObjectMapper objectMapper, Class<T> cls) {
        _serializer = new JsonSerializer<>(objectMapper);
        _deserializer = new JsonDeserializerTypedByClass<>(objectMapper, cls);
    }

    public static <T> JsonSerde<T> of(ObjectMapper objectMapper, TypeReference<T> typeRef) {
        return new JsonSerde<>(objectMapper, typeRef);
    }

    public static <T> JsonSerde<T> of(ObjectMapper objectMapper, Class<T> cls) {
        return new JsonSerde<>(objectMapper, cls);
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
