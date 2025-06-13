package cz.vut.fit.domainradar.flink.serialization;

import cz.vut.fit.domainradar.Common;
import cz.vut.fit.domainradar.flink.models.KafkaIPEntry;
import cz.vut.fit.domainradar.models.IPToProcess;
import cz.vut.fit.domainradar.serialization.JsonDeserializer;
import cz.vut.fit.domainradar.serialization.TagRegistry;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaIPEntryDeserializer
        extends CommonDeserializer
        implements KafkaRecordDeserializationSchema<KafkaIPEntry> {
    private transient Deserializer<IPToProcess> _keyDeserializer;

    private static final Logger LOG = LoggerFactory.getLogger(KafkaIPEntryDeserializer.class);

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> consumerRecord, Collector<KafkaIPEntry> collector) {
        if (_keyDeserializer == null) {
            final var mapper = Common.makeMapper().build();
            _keyDeserializer = new JsonDeserializer<>(mapper, IPToProcess.class);
        }

        IPToProcess key = _keyDeserializer.deserialize(consumerRecord.topic(), consumerRecord.key());
        var rawResult = consumerRecord.value();
        var statusMeta = this.parseStatusMeta(consumerRecord.value(), true);
        // TODO: Include the collector tag explicitly in the value.
        // byte collectorTag = value[value.length - 1];

        var collectorTagOrNull = TagRegistry.TAGS.get(statusMeta.collector());
        if (collectorTagOrNull == null) {
            LOG.info("Dropping a received entry from collector {} which was not found in the tag registry.",
                    statusMeta.collector());
            return;
        }

        var collectorTag = collectorTagOrNull.byteValue();
        collector.collect(new KafkaIPEntry(key.dn(), key.ip(), rawResult,
                statusMeta.statusCode(), statusMeta.error(), collectorTag, consumerRecord.topic(),
                consumerRecord.partition(), consumerRecord.offset(), consumerRecord.timestamp()));
    }

    @Override
    public TypeInformation<KafkaIPEntry> getProducedType() {
        return TypeInformation.of(KafkaIPEntry.class);
    }
}
