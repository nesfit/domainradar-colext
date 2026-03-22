package cz.vut.fit.domainradar.connect;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.HashMap;
import java.util.Map;

public class DomainAndTimestampsTransformation<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final ConfigDef CONFIG_DEF = new ConfigDef();

    @Override
    @SuppressWarnings("unchecked")
    public R apply(R record) {
        if (!(record.key() instanceof String keyData)) {
            throw new DataException("Expected String key");
        }

        if (!(record.value() instanceof Map<?, ?> recMap)) {
            throw new DataException("Expected schemaless Map value");
        }

        Object dnsResultObj = recMap.get("dnsResult");
        if (!(dnsResultObj instanceof Map<?, ?> dnsResult)) {
            throw new DataException("dnsResult is missing or not an object");
        }

        Object lastAttemptObj = dnsResult.get("lastAttempt");
        if (!(lastAttemptObj instanceof Number n)) {
            throw new DataException("dnsResult.lastAttempt is missing or not numeric");
        }

        long ts = n.longValue();

        Map<String, Object> updated = new HashMap<>((Map<String, Object>) recMap);
        updated.put("domainName", keyData);
        updated.put("processingStart", ts);

        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                null,
                updated,
                record.timestamp()
        );
    }
    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }
}
