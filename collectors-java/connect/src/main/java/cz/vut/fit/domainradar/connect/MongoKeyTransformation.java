package cz.vut.fit.domainradar.connect;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;
import java.util.Map;

import static cz.vut.fit.domainradar.Topics.TOPICS_TO_COLLECTOR_ID;

/**
 * Implementation of Connect's {@link Transformation} that modifies the key of a record before it is written to MongoDB.
 * <p>
 * This transformation extracts certain fields from the record's value and uses them to construct a new key.
 * The new key is either a structured key with domain name and IP or a simple domain name key, depending on whether
 * the record comes from a DN-based collector, or from an IP-based one.
 *
 * @author Ondřej Ondryáš
 */
public class MongoKeyTransformation<R extends ConnectRecord<R>> implements Transformation<R> {
    public static final Schema DN_KEY_SCHEMA = SchemaBuilder.struct()
            .name("cz.vut.fit.domainradar.MongoDNKey")
            .field("domainName", Schema.STRING_SCHEMA)
            .field("collector", Schema.STRING_SCHEMA)
            .field("timestamp", Timestamp.SCHEMA);

    public static final Schema IP_KEY_SCHEMA = SchemaBuilder.struct()
            .name("cz.vut.fit.domainradar.MongoDNIPKey")
            .field("domainName", Schema.STRING_SCHEMA)
            .field("ip", Schema.STRING_SCHEMA)
            .field("collector", Schema.STRING_SCHEMA)
            .field("timestamp", Timestamp.SCHEMA);

    private record InValueKeyData(@Nullable String collector, @Nullable Long timestamp) {
    }

    private InValueKeyData getKeyDataWithSchema(R record) {
        if (!(record.value() instanceof final Struct valueStruct)) {
            throw new DataException("Expected a structured value with a schema (a Struct)");
        }

        final var collector = valueStruct.getString("collector");
        final var timestamp = valueStruct.getInt64("lastAttempt");

        return new InValueKeyData(collector, timestamp);
    }

    @SuppressWarnings("unchecked")
    private InValueKeyData getKeyDataSchemaless(R record) {
        if (!(record.value() instanceof Map)) {
            throw new DataException("Expected an unstructured value (a Map)");
        }

        final var valueMap = (Map<String, Object>) record.value();
        final var collector = (String) valueMap.get("collector");
        final var timestamp = (Long) valueMap.get("lastAttempt");

        return new InValueKeyData(collector, timestamp);
    }

    @Override
    public R apply(R record) {
        final var keyData = record.valueSchema() == null
                ? getKeyDataSchemaless(record)
                : getKeyDataWithSchema(record);

        var collector = keyData.collector();
        if (collector == null) {
            // Determine collector from the topic name instead
            var topic = record.topic();
            collector = TOPICS_TO_COLLECTOR_ID.getOrDefault(topic, null);
        }
        if (collector == null)
            throw new DataException("Cannot determine the collector name");

        var timestamp = keyData.timestamp();
        if (timestamp == null)
            timestamp = record.timestamp();

        Struct newKeyStruct;
        if (record.keySchema().type() == Schema.Type.STRUCT &&
                record.keySchema().name().equals(IPToProcessConverter.SCHEMA.name())) {
            final var oldKeyStruct = (Struct) record.key();
            newKeyStruct = new Struct(IP_KEY_SCHEMA.schema());
            newKeyStruct.put("domainName", oldKeyStruct.getString("domain_name"));
            newKeyStruct.put("ip", oldKeyStruct.getString("ip"));
            newKeyStruct.put("collector", collector);
            newKeyStruct.put("timestamp", java.util.Date.from(Instant.ofEpochMilli(timestamp)));
        } else if (record.keySchema().type() == Schema.Type.STRING) {
            var domainName = (String) record.key();
            newKeyStruct = new Struct(DN_KEY_SCHEMA.schema());
            newKeyStruct.put("domainName", domainName);
            newKeyStruct.put("collector", collector);
            newKeyStruct.put("timestamp", java.util.Date.from(Instant.ofEpochMilli(timestamp)));
        } else {
            throw new DataException("Unexpected key schema");
        }

        return record.newRecord(record.topic(), record.kafkaPartition(), newKeyStruct.schema(), newKeyStruct,
                record.valueSchema(), record.value(), record.timestamp());
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }
}
