package cz.vut.fit.domainradar.connect;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import cz.vut.fit.domainradar.Common;
import cz.vut.fit.domainradar.Topics;
import cz.vut.fit.domainradar.standalone.collectors.DNSCollector;
import cz.vut.fit.domainradar.standalone.collectors.ZoneCollector;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public class CommonResultConverter implements Converter {
    public record CommonResult(
            int statusCode,
            @Nullable String error,
            @NotNull Instant lastAttempt,
            @Nullable String collector
    ) {
    }


    private final ObjectMapper _objectMapper;
    public static final Schema SCHEMA;
    private static final Map<String, String> TOPICS_TO_COLLECTOR_ID = new HashMap<>();

    static {
        SCHEMA = SchemaBuilder.struct()
                .name("cz.vut.fit.domainradar.CommonResult")
                .field("status_code", Schema.INT16_SCHEMA)
                .field("error", Schema.OPTIONAL_STRING_SCHEMA)
                .field("last_attempt", Timestamp.SCHEMA)
                .field("collector", Schema.OPTIONAL_STRING_SCHEMA)
                .build();

        // TODO: Make configurable.
        TOPICS_TO_COLLECTOR_ID.put(Topics.OUT_DNS, DNSCollector.NAME);
        TOPICS_TO_COLLECTOR_ID.put(Topics.OUT_ZONE, ZoneCollector.NAME);
        TOPICS_TO_COLLECTOR_ID.put(Topics.OUT_RDAP_DN, "rdap-dn");
    }


    public CommonResultConverter() {
        _objectMapper = Common.makeMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .build();
    }

    @Override
    public SchemaAndValue toConnectData(@NotNull String topic, @Nullable Headers headers, byte[] value) {
        @Nullable var collectorId = TOPICS_TO_COLLECTOR_ID.get(topic);

        try {
            var result = _objectMapper.readValue(value, CommonResult.class);
            if (result.collector == null && collectorId == null)
                throw new DataException("Cannot determine collector name");

            var resultStruct = new Struct(SCHEMA.schema());
            resultStruct.put("status_code", (short) result.statusCode());
            resultStruct.put("error", result.error());
            resultStruct.put("last_attempt", java.util.Date.from(result.lastAttempt()));
            resultStruct.put("collector", (result.collector != null) ? result.collector : collectorId);
            return new SchemaAndValue(SCHEMA.schema(), resultStruct);
        } catch (IOException e) {
            throw new DataException("Failed to read topic message", e);
        }
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // Make mapper configurable?
    }

    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        return this.toConnectData(topic, null, value);
    }

    @Override
    public byte[] fromConnectData(@Nullable String topic, @Nullable Headers headers, @Nullable Schema schema,
                                  @Nullable Object value) {
        // This converter is one-way only.
        return null;
    }

    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        // This converter is one-way only.
        return null;
    }
}
