package cz.vut.fit.domainradar.connect;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.vut.fit.domainradar.Common;
import cz.vut.fit.domainradar.Topics;
import cz.vut.fit.domainradar.models.DNToProcess;
import cz.vut.fit.domainradar.models.IPToProcess;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;

/**
 * Implementation of {@link Transformation} that produces records suitable for the PostgreSQL sink connector.
 * <p>
 * The transformation expects collector result entries (both DN- and IP-based) in their serialized form. It deserializes
 * both the key and the value and outputs a new record to be ingested by the properly configured sink connector which
 * inserts the record into the table for collection results.
 * <p>
 * The key follows {@link #KEY_SCHEMA} and contains the "primary key" fields – DN, IP, collector name and timestamp.
 * The IP is {@code null} if the result comes from a DN-based collector.
 * <p>
 * The value follows {@link #VALUE_SCHEMA} that contains the status code, the error message and the raw result data,
 * i.e. the entire JSON string with the result.
 * @author Ondřej Ondryáš
 * @author Matěj Čech
 */
public class PostgresCollectorResultSinkTransformation<R extends ConnectRecord<R>> implements Transformation<R> {
    public record CommonResult(
            int statusCode,
            @Nullable String error,
            @NotNull Instant lastAttempt,
            @Nullable String collector
    ) {
    }

    public static final Schema KEY_SCHEMA = SchemaBuilder.struct()
            .name("cz.vut.fit.domainradar.PostgresCollectedDataKey")
            .field("domain_name", Schema.STRING_SCHEMA)
            .field("ip", Schema.OPTIONAL_STRING_SCHEMA)
            .field("collector", Schema.STRING_SCHEMA)
            .field("timestamp", Schema.INT64_SCHEMA);

    public static final Schema VALUE_SCHEMA = SchemaBuilder.struct()
            .name("cz.vut.fit.domainradar.PostgresCollectedDataValue")
            .field("status_code", Schema.INT16_SCHEMA)
            .field("error", Schema.OPTIONAL_STRING_SCHEMA)
            .field("raw_data", Schema.OPTIONAL_STRING_SCHEMA);
    private final ObjectMapper _mapper;

    public PostgresCollectorResultSinkTransformation() {
        _mapper = Common.makeMapper().build();
    }

    @Override
    public R apply(R record) {
        if (record.keySchema().type() != Schema.Type.BYTES
                || record.valueSchema().type() != Schema.Type.BYTES)
            throw new DataException("Expected the BYTES schema for both the key and the value");

        final var topic = record.topic();
        final var isIpResult = topic.equals(Topics.OUT_IP) || topic.equals(Topics.OUT_QRADAR);
        final var isRepSystemDnResult = topic.equals(Topics.OUT_DN);
        final var key = (byte[]) record.key();
        final var value = (byte[]) record.value();

        try {
            final var result = _mapper.readValue(value, CommonResult.class);

            String domainName, ip, collector;
            if (isIpResult) {
                var ipData = _mapper.readValue(key, IPToProcess.class);
                domainName = ipData.dn();
                ip = ipData.ip();
                collector = result.collector;

                // Append "-ip" suffix to the collector name if the collector collects data for both IPs and DNs
                if (CombinedRepSystemCollectors.COMBINED_REP_SYSTEM_COLLECTORS.contains(collector)) {
                    collector += "-ip";
                }
            } else if (isRepSystemDnResult) {
                var dnData = _mapper.readValue(key, DNToProcess.class);
                domainName = dnData.dn();
                ip = null;
                collector = result.collector;

                // Append "-dn" suffix to the collector name if the collector collects data for both IPs and DNs
                if (CombinedRepSystemCollectors.COMBINED_REP_SYSTEM_COLLECTORS.contains(collector)) {
                    collector += "-dn";
                }
            } else {
                domainName = new String(key, StandardCharsets.UTF_8);
                ip = null;
                collector = Topics.TOPICS_TO_COLLECTOR_ID.get(topic);
            }

            final var keyStruct = new Struct(KEY_SCHEMA);
            final var valueStruct = new Struct(VALUE_SCHEMA);

            keyStruct.put("domain_name", domainName);
            keyStruct.put("ip", ip);
            keyStruct.put("collector", collector);
            keyStruct.put("timestamp", result.lastAttempt().toEpochMilli());

            valueStruct.put("status_code", (short) result.statusCode);
            valueStruct.put("error", result.error);
            valueStruct.put("raw_data", new String(value, StandardCharsets.UTF_8));

            return record.newRecord(topic, record.kafkaPartition(), KEY_SCHEMA, keyStruct, VALUE_SCHEMA, valueStruct,
                    record.timestamp());
        } catch (IOException e) {
            throw new DataException("Failed to read topic message", e);
        }
    }

    @Override
    public void close() {
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }
}
