package cz.vut.fit.domainradar.models.results;

import cz.vut.fit.domainradar.models.dns.ZoneInfo;

public record FinalResult(
        ExtendedDNSResult dnsResult,
        RDAPDomainResult rdapDomainResult,
        ZoneInfo zone
) {
}
