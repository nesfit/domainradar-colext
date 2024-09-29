package cz.vut.fit.domainradar;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.vut.fit.domainradar.serialization.JsonSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.jetbrains.annotations.Nullable;

public class KafkaSerializer<K, V> implements KafkaRecordSerializationSchema<Tuple2<K, V>> {
    private final Serializer<K> _keySerializer;
    private final Serializer<V> _valueSerializer;
    private final String _targetTopic;

    public KafkaSerializer(String targetTopic) {
        var objectMapper = Common.makeMapper().build();
        _keySerializer = new JsonSerializer<>(objectMapper);
        _valueSerializer = new JsonSerializer<>(objectMapper);
        _targetTopic = targetTopic;
    }


    @Nullable
    @Override
    public ProducerRecord<byte[], byte[]> serialize(Tuple2<K, V> element, KafkaSinkContext context, Long timestamp) {
        byte[] key = _keySerializer.serialize(_targetTopic, element.f0);
        byte[] value = _valueSerializer.serialize(_targetTopic, element.f1);
        return new ProducerRecord<>(_targetTopic, null, timestamp, key, value, null);
    }
}
