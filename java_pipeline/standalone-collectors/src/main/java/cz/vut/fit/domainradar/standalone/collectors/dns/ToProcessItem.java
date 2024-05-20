package cz.vut.fit.domainradar.standalone.collectors.dns;

import cz.vut.fit.domainradar.models.dns.ZoneInfo;

public record ToProcessItem(String domainName, ZoneInfo zoneInfo, String recordType) {
}
