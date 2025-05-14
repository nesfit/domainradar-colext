package cz.vut.fit.domainradar;

import org.jetbrains.annotations.NotNull;

/**
 * A string representing a Kafka record with a domain name key.
 * It contains the key fields, value, topic, partition, offset, and timestamp.
 *
 * @author Matěj Čech
 */
public class KafkaRepSystemDNEntry implements KafkaEntry {
    @NotNull
    String domainName;
    byte @NotNull [] value;
    int statusCode;
    Byte collectorTag;
    @NotNull
    String topic;
    int partition;
    long offset;
    long timestamp;

    public KafkaRepSystemDNEntry() {
        this.domainName = "";
        this.value = new byte[0];
        this.topic = "";
    }

    public KafkaRepSystemDNEntry(@NotNull String domainName, byte @NotNull [] value, int statusCode, Byte collectorTag,
                                 @NotNull String topic, int partition, long offset, long timestamp) {
        this.domainName = domainName;
        this.value = value;
        this.statusCode = statusCode;
        this.collectorTag = collectorTag;
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

    public byte @NotNull [] getValue() {
        return this.value;
    }

    public void setValue(byte @NotNull [] value) {
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

    public Byte getCollectorTag() {
        return this.collectorTag;
    }

    public void setCollectorTag(Byte collectorTag) {
        this.collectorTag = collectorTag;
    }
}
