package cz.vut.fit.domainradar;

public class KafkaMergedResult {

    private String domainName;
    private KafkaDomainAggregate domainData;
    // TODO: IP data

    public KafkaMergedResult(String domainName, KafkaDomainAggregate domainData) {
        this.domainName = domainName;
        this.domainData = domainData;
    }

    public String getDomainName() {
        return domainName;
    }

    public void setDomainName(String domainName) {
        this.domainName = domainName;
    }

    public KafkaDomainAggregate getDomainData() {
        return domainData;
    }

    public void setDomainData(KafkaDomainAggregate domainData) {
        this.domainData = domainData;
    }
}
