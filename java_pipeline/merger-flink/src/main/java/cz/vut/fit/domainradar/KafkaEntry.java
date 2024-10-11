package cz.vut.fit.domainradar;

import org.jetbrains.annotations.NotNull;

public interface KafkaEntry {
    @NotNull
    String getDomainName();

    byte @NotNull [] getValue();

    int getStatusCode();

    @NotNull
    String getTopic();

    int getPartition();

    long getOffset();

    long getTimestamp();
}
