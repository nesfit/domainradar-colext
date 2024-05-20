package cz.vut.fit.domainradar.standalone.collectors.dns;

public record ProcessedItem(String domainName, String recordType, Object value, long ttl) {
}
