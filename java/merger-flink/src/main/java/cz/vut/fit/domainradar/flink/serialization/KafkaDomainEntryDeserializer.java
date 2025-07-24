package cz.vut.fit.domainradar.flink.serialization;

import cz.vut.fit.domainradar.flink.models.KafkaDomainEntry;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.charset.StandardCharsets;

public class KafkaDomainEntryDeserializer
        extends CommonDeserializer
        implements KafkaRecordDeserializationSchema<KafkaDomainEntry> {

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> consumerRecord, Collector<KafkaDomainEntry> collector) {
        var key = new String(consumerRecord.key(), StandardCharsets.UTF_8);
        var statusMeta = this.parseStatusMeta(consumerRecord.value(), false);

        collector.collect(new KafkaDomainEntry(key, consumerRecord.value(), statusMeta.statusCode(), statusMeta.error(),
                consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset(), consumerRecord.timestamp()));
    }

    @Override
    public TypeInformation<KafkaDomainEntry> getProducedType() {
        return TypeInformation.of(KafkaDomainEntry.class);
    }


}
