package cz.vut.fit.domainradar.serialization;

import com.fasterxml.jackson.databind.json.JsonMapper;
import cz.vut.fit.domainradar.models.StringPair;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class StringPairSerde implements Serde<StringPair> {

    // TODO: implement effective byte-array-based serde!

    public static StringPairSerde build() {
        return new StringPairSerde();
    }

    private final JsonSerde<StringPair> _serde;

    public StringPairSerde() {
        _serde = new JsonSerde<>(JsonMapper.builder().build(), StringPair.class);
    }

    @Override
    public Serializer<StringPair> serializer() {
        return _serde.serializer();
    }

    @Override
    public Deserializer<StringPair> deserializer() {
        return _serde.deserializer();
    }
}
