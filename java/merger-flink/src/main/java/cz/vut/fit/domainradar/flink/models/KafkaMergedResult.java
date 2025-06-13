package cz.vut.fit.domainradar.flink.models;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Map;

public class KafkaMergedResult {

    @NotNull
    private String domainName;
    @NotNull
    private KafkaDomainAggregate domainData;
    @Nullable
    private Map<String, Map<Byte, KafkaIPEntry>> ipData;

    public KafkaMergedResult() {
        this.domainName = "";
        this.domainData = new KafkaDomainAggregate();
        this.ipData = null;
    }

    public KafkaMergedResult(@NotNull String domainName,
                             @NotNull KafkaDomainAggregate domainData,
                             @Nullable Map<String, Map<Byte, KafkaIPEntry>> ipData) {
        this.domainName = domainName;
        this.domainData = domainData;
        this.ipData = ipData;
    }

    public @NotNull String getDomainName() {
        return domainName;
    }

    public void setDomainName(@NotNull String domainName) {
        this.domainName = domainName;
    }

    public @NotNull KafkaDomainAggregate getDomainData() {
        return domainData;
    }

    public void setDomainData(@NotNull KafkaDomainAggregate domainData) {
        this.domainData = domainData;
    }

    public @Nullable Map<String, Map<Byte, KafkaIPEntry>> getIPData() {
        return ipData;
    }

    public void setIPData(@Nullable Map<String, Map<Byte, KafkaIPEntry>> ipData) {
        this.ipData = ipData;
    }
}
