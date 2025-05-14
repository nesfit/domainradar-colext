package cz.vut.fit.domainradar;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import cz.vut.fit.domainradar.models.DNToProcess;
import cz.vut.fit.domainradar.models.results.CommonDNResult;
import cz.vut.fit.domainradar.serialization.JsonDeserializer;
import cz.vut.fit.domainradar.serialization.TagRegistry;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Deserializer for {@link KafkaRepSystemDNEntry}.
 *
 * @author Matěj Čech
 */
public class KafkaRepSystemDNEntryDeserializer
        extends CommonDeserializer
        implements KafkaRecordDeserializationSchema<KafkaRepSystemDNEntry> {
    private transient Deserializer<DNToProcess> _keyDeserializer;
    private transient Deserializer<CommonDNResult<JsonNode>> _dnResultDeserializer;

    private static final Logger LOG = LoggerFactory.getLogger(KafkaRepSystemDNEntryDeserializer.class);

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> consumerRecord, Collector<KafkaRepSystemDNEntry> collector) {
        if (_keyDeserializer == null) {
            final var mapper = Common.makeMapper().build();
            _keyDeserializer = new JsonDeserializer<>(mapper, DNToProcess.class);

            _dnResultDeserializer = new JsonDeserializer<>(mapper, new TypeReference<>(){});
        }

        DNToProcess key = _keyDeserializer.deserialize(consumerRecord.topic(), consumerRecord.key());

        var value = consumerRecord.value();
        int statusCode = parseStatusCode(value);

        var deserialized = _dnResultDeserializer.deserialize(consumerRecord.topic(), value);
        var collectorTagOrNull = TagRegistry.REP_SYSTEM_DN_TAGS.get(deserialized.collector());
        if (collectorTagOrNull == null) {
            LOG.debug("Dropping a received entry from collector {} which was not found in the tag registry.",
                    deserialized.collector());
            return;
        }
        var collectorTag = collectorTagOrNull.byteValue();
        collector.collect(new KafkaRepSystemDNEntry(key.dn(), consumerRecord.value(), statusCode,
                collectorTag, consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset(),
                consumerRecord.timestamp()));
    }

    @Override
    public TypeInformation<KafkaRepSystemDNEntry> getProducedType() {
        return TypeInformation.of(KafkaRepSystemDNEntry.class);
    }
}
