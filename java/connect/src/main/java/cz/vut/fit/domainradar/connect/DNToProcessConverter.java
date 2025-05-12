package cz.vut.fit.domainradar.connect;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.vut.fit.domainradar.Common;
import cz.vut.fit.domainradar.models.DNToProcess;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.Map;

/**
 * Implementation of Connect's {@link Converter} that deserializes the {@link DNToProcess} DN into
 * a Connect record.
 *
 * @author Matěj Čech
 */
public class DNToProcessConverter implements Converter {
    private final ObjectMapper _objectMapper;
    public static final Schema SCHEMA = SchemaBuilder.struct()
            .name("cz.vut.fit.domainradar.DNToProcess")
            .field("domain_name", Schema.STRING_SCHEMA);

    public DNToProcessConverter() {
        _objectMapper = Common.makeMapper().build();
    }

    @Override
    public SchemaAndValue toConnectData(@NotNull String topic, @Nullable Headers headers, byte[] value) {
        try {
            var dnToProcess = _objectMapper.readValue(value, DNToProcess.class);
            var resultStruct = new Struct(SCHEMA.schema());
            resultStruct.put("domain_name", dnToProcess.dn());
            return new SchemaAndValue(SCHEMA.schema(), resultStruct);
        }
        catch (IOException e) {
            throw new DataException("Failed to read topic message", e);
        }
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        return this.toConnectData(topic, null, value);
    }

    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        // This converter is one-way only.
        return null;
    }

    @Override
    public byte[] fromConnectData(String topic, Headers headers, Schema schema, Object value) {
        // This converter is one-way only.
        return null;
    }
}
