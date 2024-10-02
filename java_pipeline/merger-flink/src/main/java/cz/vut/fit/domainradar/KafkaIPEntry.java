package cz.vut.fit.domainradar;

import org.apache.flink.api.java.tuple.Tuple7;
import org.jetbrains.annotations.NotNull;

/**
 * A tuple representing a Kafka record with a (domain name, IP) pair key.
 * It contains the key fields, value, topic, partition, offset, and timestamp.
 */
public class KafkaIPEntry extends Tuple7<String, String, byte[], String, Integer, Long, Long> {

    public KafkaIPEntry() {
        super();
    }

    public KafkaIPEntry(@NotNull String key, String ip, byte[] value, @NotNull String topic,
                        int partition, long offset, long timestamp) {
        super(key, ip, value, topic, partition, offset, timestamp);
    }

    @NotNull
    public String getDomainName() {
        return f0;
    }

    @NotNull
    public String getIP() {
        return f1;
    }

    public byte[] getValue() {
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
