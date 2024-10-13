package cz.vut.fit.domainradar.connect;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;

import java.util.Map;

/**
 * Implementation of {@link Converter} that keeps its input entry as a byte array.
 * Used in conjunction with {@link PostgresSinkTransformation}.
 *
 * @author Ondřej Ondryáš
 * @see PostgresSinkTransformation
 */
public class BytesConverter implements Converter {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        if (schema.type() == Schema.Type.BYTES)
            return (byte[]) value;
        else
            throw new DataException("Expected the BYTES schema");
    }

    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        return new SchemaAndValue(Schema.BYTES_SCHEMA, value);
    }
}
