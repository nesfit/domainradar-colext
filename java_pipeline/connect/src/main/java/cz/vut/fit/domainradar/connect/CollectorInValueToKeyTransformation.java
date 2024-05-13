package cz.vut.fit.domainradar.connect;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.Map;

public class CollectorInValueToKeyTransformation<R extends ConnectRecord<R>> implements Transformation<R> {
    public static final Schema DN_KEY_SCHEMA = SchemaBuilder.struct()
            .name("cz.vut.fit.domainradar.DNKey")
            .field("domain_name", Schema.STRING_SCHEMA)
            .field("collector", Schema.STRING_SCHEMA);

    public static final Schema IP_KEY_SCHEMA = SchemaBuilder.struct()
            .name("cz.vut.fit.domainradar.DNIPKey")
            .field("domain_name", Schema.STRING_SCHEMA)
            .field("ip", Schema.STRING_SCHEMA)
            .field("collector", Schema.STRING_SCHEMA);

    @Override
    public R apply(R record) {
        if (!record.valueSchema().name().equals(CommonResultConverter.SCHEMA.name()))
            throw new DataException("Unexpected value schema");

        var valueStruct = (Struct) record.value();
        var collector = valueStruct.getString("collector");
        if (collector == null)
            throw new DataException("Collector name is missing in the record value");

        Struct newKeyStruct;

        if (record.keySchema().type() == Schema.Type.STRUCT &&
                record.keySchema().name().equals(IPToProcessConverter.SCHEMA.name())) {
            var oldKeyStruct = (Struct) record.key();
            newKeyStruct = new Struct(IP_KEY_SCHEMA.schema());
            newKeyStruct.put("domain_name", oldKeyStruct.getString("domain_name"));
            newKeyStruct.put("ip", oldKeyStruct.getString("ip"));
            newKeyStruct.put("collector", collector);
        } else if (record.keySchema().type() == Schema.Type.STRING) {
            var domainName = (String) record.key();
            newKeyStruct = new Struct(DN_KEY_SCHEMA.schema());
            newKeyStruct.put("domain_name", domainName);
            newKeyStruct.put("collector", collector);
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
