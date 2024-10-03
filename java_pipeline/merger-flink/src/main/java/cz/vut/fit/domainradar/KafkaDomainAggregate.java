package cz.vut.fit.domainradar;

public class KafkaDomainAggregate {
    private String domainName;
    private KafkaDomainEntry zoneData;
    private KafkaDomainEntry dnsData;
    private KafkaDomainEntry rdapData;
    private KafkaDomainEntry tlsData;

    public KafkaDomainAggregate() {
    }

    public KafkaDomainAggregate(String domainName, KafkaDomainEntry zoneData, KafkaDomainEntry dnsData, KafkaDomainEntry rdapDnData, KafkaDomainEntry tlsData) {
        this.domainName = domainName;
        this.zoneData = zoneData;
        this.dnsData = dnsData;
        this.rdapData = rdapDnData;
        this.tlsData = tlsData;
    }

    public String getDomainName() {
        return domainName;
    }

    public void setDomainName(String domainName) {
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

    public boolean isComplete() {
        return zoneData != null && dnsData != null && rdapData != null && tlsData != null;
    }
}
