package cz.vut.fit.domainradar;

import java.util.Map;

public class KafkaMergedResult {

    private String domainName;
    private KafkaDomainAggregate domainData;
    private Map<String, Map<Byte, KafkaIPEntry>> ipData;

    public KafkaMergedResult(String domainName,
                             KafkaDomainAggregate domainData,
                             Map<String, Map<Byte, KafkaIPEntry>> ipData) {
        this.domainName = domainName;
        this.domainData = domainData;
        this.ipData = ipData;
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

    public Map<String, Map<Byte, KafkaIPEntry>> getIPData() {
        return ipData;
    }

    public void setIPData(Map<String, Map<Byte, KafkaIPEntry>> ipData) {
        this.ipData = ipData;
    }
}
