package cz.vut.fit.domainradar;

import cz.vut.fit.domainradar.models.IPToProcess;
import cz.vut.fit.domainradar.serialization.JsonDeserializer;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Deserializer;

public class KafkaIPEntryDeserializer implements KafkaRecordDeserializationSchema<KafkaIPEntry> {
    private transient Deserializer<IPToProcess> _keyDeserializer;

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> consumerRecord, Collector<KafkaIPEntry> collector) {
        if (_keyDeserializer == null) {
            final var mapper = Common.makeMapper().build();
            _keyDeserializer = new JsonDeserializer<>(mapper, IPToProcess.class);
        }

        IPToProcess key = _keyDeserializer.deserialize(consumerRecord.topic(), consumerRecord.key());
        collector.collect(new KafkaIPEntry(key.dn(), key.ip(), consumerRecord.value(), consumerRecord.topic(),
                consumerRecord.partition(), consumerRecord.offset(), consumerRecord.timestamp()));
    }

    @Override
    public TypeInformation<KafkaIPEntry> getProducedType() {
        return TypeInformation.of(KafkaIPEntry.class);
    }
}
