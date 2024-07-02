package cz.vut.fit.domainradar.models.results;

import com.fasterxml.jackson.databind.JsonNode;
import cz.vut.fit.domainradar.models.dns.ZoneInfo;
import org.jetbrains.annotations.Nullable;

import java.util.Map;

public record AllCollectedData(
        ZoneInfo zone,
        DNSResult dnsResult,
        @Nullable TLSResult tlsResult,
        @Nullable RDAPDomainResult rdapDomainResult,
        @Nullable Map<String, Map<String, CommonIPResult<JsonNode>>> ipResults
) {
}
