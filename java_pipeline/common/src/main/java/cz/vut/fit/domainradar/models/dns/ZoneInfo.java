package cz.vut.fit.domainradar.models.dns;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Set;

/**
 * A record representing information about a DNS zone.
 *
 * @param zone                   The name of the DNS zone.
 * @param soa                    The SOA (Start of Authority) record for the DNS zone.
 * @param publicSuffix           The public suffix of the DNS zone.
 * @param hasDNSKEY              Indicates whether the DNS zone has a DNSKEY record.
 * @param primaryNameserverIPs   A set of IP addresses for the primary nameservers.
 * @param secondaryNameservers   A set of secondary nameservers.
 * @param secondaryNameserverIPs A set of IP addresses for the secondary nameservers.
 * @author Ondřej Ondryáš
 */
public record ZoneInfo(
        @NotNull String zone,
        @NotNull DNSData.SOARecord soa,
        @NotNull String publicSuffix,
        @Nullable Boolean hasDNSKEY,
        @NotNull Set<String> primaryNameserverIPs,
        @NotNull Set<String> secondaryNameservers,
        @NotNull Set<String> secondaryNameserverIPs
) {
}
