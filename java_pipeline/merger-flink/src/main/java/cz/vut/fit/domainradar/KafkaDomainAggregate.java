package cz.vut.fit.domainradar;

public class KafkaDomainAggregate {
    private String domainName;
    private KafkaDomainEntry zoneData;
    private KafkaDomainEntry dnsData;
    private KafkaDomainEntry rdapDnData;
    private KafkaDomainEntry tlsData;

    public KafkaDomainAggregate() {
    }

    public KafkaDomainAggregate(String domainName, KafkaDomainEntry zoneData, KafkaDomainEntry dnsData, KafkaDomainEntry rdapDnData, KafkaDomainEntry tlsData) {
        this.domainName = domainName;
        this.zoneData = zoneData;
        this.dnsData = dnsData;
        this.rdapDnData = rdapDnData;
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

    public KafkaDomainEntry getDnsData() {
        return dnsData;
    }

    public void setDnsData(KafkaDomainEntry dnsData) {
        this.dnsData = dnsData;
    }

    public KafkaDomainEntry getRdapDnData() {
        return rdapDnData;
    }

    public void setRdapDnData(KafkaDomainEntry rdapDnData) {
        this.rdapDnData = rdapDnData;
    }

    public KafkaDomainEntry getTlsData() {
        return tlsData;
    }

    public void setTlsData(KafkaDomainEntry tlsData) {
        this.tlsData = tlsData;
    }

    public boolean isComplete() {
        return zoneData != null && dnsData != null && rdapDnData != null && tlsData != null;
    }
}
