package cz.vut.fit.domainradar;

import org.apache.flink.api.java.tuple.Tuple7;
import org.jetbrains.annotations.NotNull;

/**
 * A tuple representing a Kafka record with a string key.
 * It contains the key, value, topic, partition, offset, and timestamp.
 */
public class KafkaDomainEntry extends Tuple7<String, byte[], Integer, String, Integer, Long, Long> {

    public KafkaDomainEntry() {
        super();
    }

    public KafkaDomainEntry(@NotNull String key, byte[] value, int statusCode, @NotNull String topic,
                            int partition, long offset, long timestamp) {
        super(key, value, statusCode, topic, partition, offset, timestamp);
    }

    @NotNull
    public String getDomainName() {
        return f0;
    }

    public byte[] getValue() {
        return f1;
    }

    public int getStatusCode() {
        return f2;
    }

    @NotNull
    public String getTopic() {
        return f3;
    }

    public int getPartition() {
        return f4;
    }

    public long getOffset() {
        return f5;
    }

    public long getTimestamp() {
        return f6;
    }
}
