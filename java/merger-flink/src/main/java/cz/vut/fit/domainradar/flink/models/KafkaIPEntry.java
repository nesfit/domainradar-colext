package cz.vut.fit.domainradar.flink.models;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A POJO representing an IP-based collector result entry in Kafka.
 * It contains the key formed as a (domain name, IP) pair, value, topic, partition, offset, and timestamp.
 * The collector is identified by a collector tag, as registered in the {@link cz.vut.fit.domainradar.serialization.TagRegistry}.
 * The data of the entry is kept in its serialized, byte-array form,
 * as it is not necessary to deserialize it for the merger's operations.
 */
public class KafkaIPEntry implements KafkaEntry {

    @NotNull
    String domainName;
    @NotNull
    String ip;
    byte @NotNull [] value;
    int statusCode;
    @Nullable
    String error;
    byte collectorTag;
    @NotNull
    String topic;
    int partition;
    long offset;
    long timestamp;

    public KafkaIPEntry() {
        this.domainName = "";
        this.ip = "";
        this.topic = "";
        this.value = new byte[0];
    }

    public KafkaIPEntry(@NotNull String domainName, @NotNull String ip, byte @NotNull [] value, int statusCode,
                        @Nullable String error, byte collectorTag,
                        @NotNull String topic, int partition, long offset, long timestamp) {
        this.domainName = domainName;
        this.ip = ip;
        this.value = value;
        this.statusCode = statusCode;
        this.error = error;
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

    @NotNull
    public String getIP() {
        return this.ip;
    }

    public void setIP(@NotNull String ip) {
        this.ip = ip;
    }

    /**
     * Returns the collector tag, which is a byte value representing the collector.
     * The tag is registered in the {@link cz.vut.fit.domainradar.serialization.TagRegistry}.
     *
     * @return The collector tag.
     */
    public byte getCollectorTag() {
        return this.collectorTag;
    }

    /**
     * Sets the collector tag, which is a byte value representing the collector.
     * The tag should be registered in the {@link cz.vut.fit.domainradar.serialization.TagRegistry}.
     *
     * @param collectorTag The collector tag to set.
     */
    public void setCollectorTag(byte collectorTag) {
        this.collectorTag = collectorTag;
    }

    public @Nullable String getError() {
        return error;
    }

    public void setError(@Nullable String error) {
        this.error = error;
    }
}
