package cz.vut.fit.domainradar.flink.serialization;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import cz.vut.fit.domainradar.Common;
import cz.vut.fit.domainradar.flink.models.KafkaIPEntry;
import cz.vut.fit.domainradar.models.IPToProcess;
import cz.vut.fit.domainradar.models.results.CommonIPResult;
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
    private transient Deserializer<CommonIPResult<JsonNode>> _ipResultDeserializer;

    private static final Logger LOG = LoggerFactory.getLogger(KafkaIPEntryDeserializer.class);

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> consumerRecord, Collector<KafkaIPEntry> collector) {
        if (_keyDeserializer == null) {
            final var mapper = Common.makeMapper().build();
            _keyDeserializer = new JsonDeserializer<>(mapper, IPToProcess.class);
            _ipResultDeserializer = new JsonDeserializer<>(mapper, new TypeReference<>() {
            });
        }

        IPToProcess key = _keyDeserializer.deserialize(consumerRecord.topic(), consumerRecord.key());

        var value = consumerRecord.value();

        // TODO: Include the collector tag directly in the value. Use the tag directly for KafkaIPEntry
        //       instead of deserializing the value as CommonIPResult<JsonNode> and parse the statusCode
        //       and error using parseStatusMeta.
        // byte collectorTag = value[value.length - 1];

        var deserialized = _ipResultDeserializer.deserialize(consumerRecord.topic(), value);
        var collectorTagOrNull = TagRegistry.TAGS.get(deserialized.collector());
        if (collectorTagOrNull == null) {
            LOG.debug("Dropping a received entry from collector {} which was not found in the tag registry.",
                    deserialized.collector());
            return;
        }

        var collectorTag = collectorTagOrNull.byteValue();
        collector.collect(new KafkaIPEntry(key.dn(), key.ip(), consumerRecord.value(),
                deserialized.statusCode(), deserialized.error(), collectorTag, consumerRecord.topic(),
                consumerRecord.partition(), consumerRecord.offset(), consumerRecord.timestamp()));
    }

    @Override
    public TypeInformation<KafkaIPEntry> getProducedType() {
        return TypeInformation.of(KafkaIPEntry.class);
    }
}
