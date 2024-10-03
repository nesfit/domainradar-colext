package cz.vut.fit.domainradar;

import org.jetbrains.annotations.NotNull;

/**
 * A POJO representing a Kafka record with a string key.
 * It contains the key, value, status (result) code, topic, partition, offset, and timestamp.
 */
public class KafkaDomainEntry implements KafkaEntry {

    @NotNull
    String domainName;
    byte[] value;
    int statusCode;
    @NotNull
    String topic;
    int partition;
    long offset;
    long timestamp;

    public KafkaDomainEntry() {
        this.domainName = "";
        this.topic = "";
        this.value = new byte[0];
    }

    public KafkaDomainEntry(@NotNull String domainName, byte[] value, int statusCode, @NotNull String topic,
                            int partition, long offset, long timestamp) {
        this.domainName = domainName;
        this.value = value;
        this.statusCode = statusCode;
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.timestamp = timestamp;
    }

    @NotNull
    public String getDomainName() {
        return this.domainName;
    }

    public void setDomainName(@NotNull String domainName) {
        this.domainName = domainName;
    }

    public byte[] getValue() {
        return this.value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    public int getStatusCode() {
        return this.statusCode;
    }

    public void setStatusCode(int statusCode) {
        this.statusCode = statusCode;
    }

    @NotNull
    public String getTopic() {
        return this.topic;
    }

    public void setTopic(@NotNull String topic) {
        this.topic = topic;
    }

    public int getPartition() {
        return this.partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public long getOffset() {
        return this.offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public long getTimestamp() {
        return this.timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
