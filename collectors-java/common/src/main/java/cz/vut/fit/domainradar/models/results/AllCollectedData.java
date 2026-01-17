package cz.vut.fit.domainradar.models.results;

import com.fasterxml.jackson.databind.JsonNode;
import cz.vut.fit.domainradar.models.dns.ZoneInfo;
import org.jetbrains.annotations.Nullable;

import java.util.Map;

/**
 * A record representing the merged data object with all the data collected for a single domain.
 *
 * @param zone             The DNS zone information.
 * @param dnsResult        The DNS result data.
 * @param tlsResult        The TLS result data, nullable.
 * @param rdapDomainResult The RDAP domain result data, nullable.
 * @param ipResults        A map of IP results, where the key is a string (denoting the IP address) and
 *                         the value is another map with a string key (denoting the source collector) and
 *                         a {@link CommonIPResult} with arbitrary data value, nullable.
 * @author Ondřej Ondryáš
 */
public record AllCollectedData(
        ZoneInfo zone,
        DNSResult dnsResult,
        @Nullable TLSResult tlsResult,
        @Nullable RDAPDomainResult rdapDomainResult,
        @Nullable Map<String, Map<String, CommonIPResult<JsonNode>>> ipResults
) {
}
