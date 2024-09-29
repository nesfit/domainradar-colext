package cz.vut.fit.domainradar;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import cz.vut.fit.domainradar.serialization.JsonDeserializer;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Deserializer;

public class KafkaDeserializer<K, V> implements KafkaRecordDeserializationSchema<ConsumedKafkaRecord<K, V>> {
    Deserializer<K> _keyDeserializer;
    Deserializer<V> _valueDeserializer;

    public KafkaDeserializer(ObjectMapper objectMapper) {
        _keyDeserializer = new JsonDeserializer<>(objectMapper, new TypeReference<K>() {
        });
        _valueDeserializer = new JsonDeserializer<>(objectMapper, new TypeReference<V>() {
        });
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> consumerRecord,
                            Collector<ConsumedKafkaRecord<K, V>> collector) {
        K key = _keyDeserializer.deserialize(consumerRecord.topic(), consumerRecord.key());
        V value = _valueDeserializer.deserialize(consumerRecord.topic(), consumerRecord.value());
        collector.collect(new ConsumedKafkaRecord<>(key, value, consumerRecord.topic(), consumerRecord.partition(),
                consumerRecord.offset(), consumerRecord.timestamp()));
    }

    @Override
    public TypeInformation<ConsumedKafkaRecord<K, V>> getProducedType() {
        return TypeInformation.of(new TypeHint<ConsumedKafkaRecord<K, V>>() {
        });
    }
}
