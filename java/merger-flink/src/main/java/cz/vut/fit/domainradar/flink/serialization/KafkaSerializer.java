package cz.vut.fit.domainradar.flink.serialization;

import cz.vut.fit.domainradar.Common;
import cz.vut.fit.domainradar.serialization.JsonSerializer;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;

public class KafkaSerializer<K> implements KafkaRecordSerializationSchema<Tuple2<K, byte[]>> {
    @NotNull
    private final String _targetTopic;
    private final boolean _isKeyString;
    private transient Serializer<K> _keySerializer;

    public KafkaSerializer(@NotNull String targetTopic, boolean isKeyString) {
        _targetTopic = targetTopic;
        _isKeyString = isKeyString;
    }

    @Override
    public void open(SerializationSchema.InitializationContext context, KafkaSinkContext sinkContext) throws Exception {
        KafkaRecordSerializationSchema.super.open(context, sinkContext);

        var objectMapper = Common.makeMapper().build();
        if (_isKeyString) {
            //noinspection unchecked
            _keySerializer = (Serializer<K>) new StringSerializer();
        } else {
            _keySerializer = new JsonSerializer<>(objectMapper);
        }
    }

    @Nullable
    @Override
    public ProducerRecord<byte[], byte[]> serialize(Tuple2<K, byte[]> element, KafkaSinkContext context, Long timestamp) {
        byte[] key = _keySerializer.serialize(_targetTopic, element.f0);
        return new ProducerRecord<>(_targetTopic, null,
                Instant.now().toEpochMilli(), key, element.f1, null);
    }
}
