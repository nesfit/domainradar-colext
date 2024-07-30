package cz.vut.fit.domainradar.serialization;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

/**
 * A custom JSON Serde for Kafka that uses Jackson to serialize and deserialize JSON data into Java objects.
 *
 * @param <T> The type of the (de)serialized object.
 * @author Ondřej Ondryáš
 */
public class JsonSerde<T> implements Serde<T> {
    private final JsonSerializer<T> _serializer;
    private final Deserializer<T> _deserializer;

    public JsonSerde(ObjectMapper objectMapper, TypeReference<T> typeRef) {
        _serializer = new JsonSerializer<>(objectMapper);
        _deserializer = new JsonDeserializer<>(objectMapper, typeRef);
    }

    public JsonSerde(ObjectMapper objectMapper, Class<T> cls) {
        _serializer = new JsonSerializer<>(objectMapper);
        _deserializer = new JsonDeserializer<>(objectMapper, cls);
    }

    /**
     * Creates a new JsonSerde instance for the specified generic type using a TypeReference.
     *
     * @param objectMapper The ObjectMapper instance to use for serialization and deserialization.
     * @param typeRef      The TypeReference representing the generic type to (de)serialize.
     * @param <T>          The type of the (de)serialized object.
     * @return A new JsonSerde instance for the specified generic type.
     */
    public static <T> JsonSerde<T> of(ObjectMapper objectMapper, TypeReference<T> typeRef) {
        return new JsonSerde<>(objectMapper, typeRef);
    }

    /**
     * Creates a new JsonSerde instance for the specified generic type using a Class.
     *
     * @param objectMapper The ObjectMapper instance to use for serialization and deserialization.
     * @param cls          The Class representing the generic type to (de)serialize.
     * @param <T>          The type of the (de)serialized object.
     * @return A new JsonSerde instance for the specified generic type.
     */
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
