package cz.vut.fit.domainradar;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import cz.vut.fit.domainradar.models.IPToProcess;
import cz.vut.fit.domainradar.models.results.CommonIPResult;
import cz.vut.fit.domainradar.serialization.JsonDeserializer;
import cz.vut.fit.domainradar.serialization.TagRegistry;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Deserializer;

public class KafkaIPEntryDeserializer
        extends CommonDeserializer
        implements KafkaRecordDeserializationSchema<KafkaIPEntry> {
    private transient Deserializer<IPToProcess> _keyDeserializer;
    private transient Deserializer<CommonIPResult<JsonNode>> _ipResultDeserializer;

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
        int statusCode = parseStatusCode(value);

        // TODO: Use tags
        // byte collectorTag = value[value.length - 1];
        var deserialized = _ipResultDeserializer.deserialize(consumerRecord.topic(), value);
        var collectorTag = TagRegistry.TAGS.get(deserialized.collector()).byteValue();

        collector.collect(new KafkaIPEntry(key.dn(), key.ip(), consumerRecord.value(),
                statusCode, collectorTag, consumerRecord.topic(), consumerRecord.partition(),
                consumerRecord.offset(), consumerRecord.timestamp()));
    }

    @Override
    public TypeInformation<KafkaIPEntry> getProducedType() {
        return TypeInformation.of(KafkaIPEntry.class);
    }
}
