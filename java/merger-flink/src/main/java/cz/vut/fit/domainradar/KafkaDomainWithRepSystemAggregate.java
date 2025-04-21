package cz.vut.fit.domainradar;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * Aggregate containing {@link KafkaDomainAggregate} + entries from reputation system about a domain name.
 *
 * @author Matěj Čech
 */
public class KafkaDomainWithRepSystemAggregate {
    @NotNull
    private String domainName;
    @NotNull
    private KafkaDomainAggregate domainData;
    private List<KafkaRepSystemDNEntry> repSystemDNEntries;

    @Override
    public String toString() {
        return "KafkaMergedResultWithRepSystem{" +
                "domainName='" + domainName + '\'' +
                ", repSystemDNEntries len: " + repSystemDNEntries.size() +
                '}';
    }

    public KafkaDomainWithRepSystemAggregate() {
        this.domainName = "";
        this.domainData = new KafkaDomainAggregate();
        this.repSystemDNEntries = new ArrayList<>();
    }

    public KafkaDomainWithRepSystemAggregate(@NotNull String domainName,
                                             @NotNull KafkaDomainAggregate domainData,
                                             @Nullable List<KafkaRepSystemDNEntry> repSystemDNEntries) {
        this.domainName = domainName;
        this.domainData = domainData;
        this.repSystemDNEntries = repSystemDNEntries;
    }

    public @NotNull String getDomainName() {
        return domainName;
    }

    public void setDomainName(@NotNull String domainName) {
        this.domainName = domainName;
    }

    public List<KafkaRepSystemDNEntry> getRepSystemDNEntries() {
        return this.repSystemDNEntries;
    }

    public void setRepSystemDNEntries(List<KafkaRepSystemDNEntry> repSystemDNEntries) {
        this.repSystemDNEntries = repSystemDNEntries;
    }

    public @NotNull KafkaDomainAggregate getDomainData() {
        return domainData;
    }

    public void setDomainData(@NotNull KafkaDomainAggregate domainData) {
        this.domainData = domainData;
    }
}
