package cz.vut.fit.domainradar;

import org.jetbrains.annotations.NotNull;

import java.util.Map;

public class KafkaMergedResult {

    @NotNull
    private String domainName;
    @NotNull
    private KafkaDomainAggregate domainData;
    @NotNull
    private Map<String, Map<Byte, KafkaIPEntry>> ipData;

    public KafkaMergedResult() {
        this.domainName = "";
        this.domainData = new KafkaDomainAggregate();
        this.ipData = Map.of();
    }

    public KafkaMergedResult(@NotNull String domainName,
                             @NotNull KafkaDomainAggregate domainData,
                             @NotNull Map<String, Map<Byte, KafkaIPEntry>> ipData) {
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

    public @NotNull Map<String, Map<Byte, KafkaIPEntry>> getIPData() {
        return ipData;
    }

    public void setIPData(@NotNull Map<String, Map<Byte, KafkaIPEntry>> ipData) {
        this.ipData = ipData;
    }
}
