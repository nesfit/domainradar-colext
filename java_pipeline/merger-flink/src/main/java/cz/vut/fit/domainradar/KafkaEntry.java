package cz.vut.fit.domainradar;

public interface KafkaEntry {
    String getDomainName();

    byte[] getValue();

    int getStatusCode();

    String getTopic();

    int getPartition();

    long getOffset();

    long getTimestamp();
}
