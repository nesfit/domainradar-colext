package cz.vut.fit.domainradar.models.results;

import cz.vut.fit.domainradar.models.dns.ZoneInfo;

public record FinalResult(
        ExtendedDNSResult dnsResult,
        TLSResult tlsResult,
        RDAPDomainResult rdapDomainResult,
        ZoneInfo zone
) {
}
