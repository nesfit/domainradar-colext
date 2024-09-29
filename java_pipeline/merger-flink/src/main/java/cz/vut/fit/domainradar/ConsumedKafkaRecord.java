package cz.vut.fit.domainradar;

import org.apache.flink.api.java.tuple.Tuple6;

/**
 * A tuple representing a deserialized Kafka record.
 * It contains the key, value, topic, partition, offset, and timestamp.
 * @param <K> Key type
 * @param <V> Value type
 */
public class ConsumedKafkaRecord<K, V> extends Tuple6<K, V, String, Integer, Long, Long> {

    public ConsumedKafkaRecord() {
        super();
    }

    public ConsumedKafkaRecord(K key, V value, String topic, int partition, long offset, long timestamp) {
        super(key, value, topic, partition, offset, timestamp);
    }

    public K getKey() {
        return f0;
    }

    public V getValue() {
        return f1;
    }

    public String getTopic() {
        return f2;
    }

    public int getPartition() {
        return f3;
    }

    public long getOffset() {
        return f4;
    }

    public long getTimestamp() {
        return f5;
    }
}
