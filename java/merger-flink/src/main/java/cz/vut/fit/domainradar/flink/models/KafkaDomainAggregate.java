package cz.vut.fit.domainradar.flink.models;

import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;

/**
 * A POJO representing a container for the results of all domain-based collectors (zone, DNS, RDAP-DN, TLS).
 */
public class KafkaDomainAggregate {
    @NotNull
    private String domainName;
    private KafkaDomainEntry zoneData;
    private KafkaDomainEntry dnsData;
    private KafkaDomainEntry rdapData;
    private KafkaDomainEntry tlsData;
    @NotNull
    private List<String> dnsIps;

    @Override
    public String toString() {
        return "KafkaDomainAggregate[" + (isMaybeComplete() ? "MC" : "IN") + "]{" +
                "domainName='" + domainName + '\'' +
                ", zoneData: " + (zoneData == null ? "null" : (zoneData.statusCode + "/" + zoneData.timestamp)) +
                ", dnsData: " + (dnsData == null ? "null" : (dnsData.statusCode + "/" + dnsData.timestamp)) +
                ", rdapData: " + (rdapData == null ? "null" : (rdapData.statusCode + "/" + rdapData.timestamp)) +
                ", tlsData: " + (tlsData == null ? "null" : (tlsData.statusCode + "/" + tlsData.timestamp)) +
                ", dnsIps len: " + dnsIps.size() +
                '}';
    }

    public KafkaDomainAggregate() {
        this.domainName = "";
        this.dnsIps = new ArrayList<>();
    }

    public KafkaDomainAggregate(@NotNull String domainName, KafkaDomainEntry zoneData, KafkaDomainEntry dnsData,
                                KafkaDomainEntry rdapDnData, KafkaDomainEntry tlsData) {
        this.domainName = domainName;
        this.zoneData = zoneData;
        this.dnsData = dnsData;
        this.rdapData = rdapDnData;
        this.tlsData = tlsData;
        this.dnsIps = new ArrayList<>();
    }

    public @NotNull String getDomainName() {
        return domainName;
    }

    public void setDomainName(@NotNull String domainName) {
        this.domainName = domainName;
    }

    public KafkaDomainEntry getZoneData() {
        return zoneData;
    }

    public void setZoneData(KafkaDomainEntry zoneData) {
        this.zoneData = zoneData;
    }

    public KafkaDomainEntry getDNSData() {
        return dnsData;
    }

    public void setDNSData(KafkaDomainEntry dnsData) {
        this.dnsData = dnsData;
    }

    public KafkaDomainEntry getRDAPData() {
        return rdapData;
    }

    public void setRDAPData(KafkaDomainEntry rdapData) {
        this.rdapData = rdapData;
    }

    public KafkaDomainEntry getTLSData() {
        return tlsData;
    }

    public void setTLSData(KafkaDomainEntry tlsData) {
        this.tlsData = tlsData;
    }

    public boolean isMaybeComplete() {
        return zoneData != null && dnsData != null && rdapData != null;
    }

    public @NotNull List<String> getDNSIPs() {
        return dnsIps;
    }

    public void setDNSIPs(@NotNull List<String> dnsIps) {
        this.dnsIps = dnsIps;
    }
}
