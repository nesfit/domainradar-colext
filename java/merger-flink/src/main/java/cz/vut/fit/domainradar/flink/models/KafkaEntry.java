package cz.vut.fit.domainradar.flink.models;

import org.jetbrains.annotations.NotNull;

/**
 * Represents a DomainRadar collection result in Kafka with metadata and content.
 * This interface defines the structure of a Kafka result record, including the associated domain name,
 * the serialized value, status code, topic, partition, offset, and timestamp.
 */
public interface KafkaEntry extends HasDomainName {
    /**
     * Gets the serialized collection result.
     *
     * @return The serialized collection result as a non-null byte array.
     */
    byte @NotNull [] getValue();

    /**
     * Gets the collection result status code.
     *
     * @return The status code as an integer.
     */
    int getStatusCode();

    /**
     * Gets the topic of the Kafka entry.
     *
     * @return The topic as a non-null string.
     */
    @NotNull
    String getTopic();

    /**
     * Gets the partition number of the Kafka entry.
     *
     * @return The partition number as an integer.
     */
    int getPartition();

    /**
     * Gets the offset of the Kafka entry within its partition.
     *
     * @return The offset as a long.
     */
    long getOffset();

    /**
     * Gets the timestamp of the Kafka entry.
     *
     * @return The timestamp as a long.
     */
    long getTimestamp();
}
