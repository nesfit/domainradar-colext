package cz.vut.fit.domainradar.connect;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.Map;

/**
 * Implementation of Connect's {@link Transformation} that extracts the "domain" attribute fetched from the PostgreSQL
 * database and sets it as the key for the result record.
 *
 * @author Ondřej Ondryáš
 */
public class PostgresIngressTransformation<R extends ConnectRecord<R>> implements Transformation<R> {
    @Override
    public R apply(R r) {
        if (r == null)
            return null;

        String dn;

        if (r.value() instanceof Map) {
            // In case the value was schemaless
            @SuppressWarnings("unchecked") final var valueMap = (Map<String, Object>) r.value();
            dn = (String) valueMap.get("domain");
        } else if (r.value() instanceof Struct struct) {
            dn = struct.getString("domain");
        } else {
            throw new DataException("Unsupported value type");
        }

        if (dn == null)
            throw new DataException("Domain name is null");

        // Set partition to null to enforce the default hash-based partitioning
        // It is not required for the input topics but why not keep it all co-partitioned
        return r.newRecord(r.topic(), null, Schema.STRING_SCHEMA, dn,
                null, null, r.timestamp());
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> map) {
    }
}
