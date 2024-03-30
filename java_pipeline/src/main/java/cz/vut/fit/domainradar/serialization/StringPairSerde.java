package cz.vut.fit.domainradar.serialization;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.json.JsonMapper;
import cz.vut.fit.domainradar.models.StringPair;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class StringPairSerde<T extends StringPair> implements Serde<T> {

    // TODO: implement effective byte-array-based serde!

    public static <T extends StringPair> StringPairSerde<T> build() {
        return new StringPairSerde<>();
    }

    private final JsonSerde<T> _serde;

    public StringPairSerde() {
        var ref = new TypeReference<T>() {};
        _serde = new JsonSerde<>(JsonMapper.builder().build(), ref);
    }

    @Override
    public Serializer<T> serializer() {
        return _serde.serializer();
    }

    @Override
    public Deserializer<T> deserializer() {
        return _serde.deserializer();
    }
}
