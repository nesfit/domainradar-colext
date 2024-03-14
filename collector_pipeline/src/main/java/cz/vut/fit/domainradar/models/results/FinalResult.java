package cz.vut.fit.domainradar.models.results;

public record FinalResult(
        ExtendedDNSResult dnsResult,
        RDAPDomainResult rdapDomainResult
) {
}
