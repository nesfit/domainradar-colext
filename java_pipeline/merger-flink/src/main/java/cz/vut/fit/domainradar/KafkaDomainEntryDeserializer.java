package cz.vut.fit.domainradar;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;

public class KafkaDomainEntryDeserializer
        extends CommonDeserializer
        implements KafkaRecordDeserializationSchema<KafkaDomainEntry> {
    private transient Deserializer<String> _keyDeserializer;

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> consumerRecord, Collector<KafkaDomainEntry> collector) {
        if (_keyDeserializer == null) {
            _keyDeserializer = new StringDeserializer();
        }

        String key = _keyDeserializer.deserialize(consumerRecord.topic(), consumerRecord.key());
        int statusCode = parseStatusCode(consumerRecord.value());

        collector.collect(new KafkaDomainEntry(key, consumerRecord.value(), statusCode, consumerRecord.topic(),
                consumerRecord.partition(), consumerRecord.offset(), consumerRecord.timestamp()));
    }

    @Override
    public TypeInformation<KafkaDomainEntry> getProducedType() {
        return TypeInformation.of(KafkaDomainEntry.class);
    }


}
