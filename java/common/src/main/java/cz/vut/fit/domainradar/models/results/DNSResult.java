package cz.vut.fit.domainradar.models.results;

import cz.vut.fit.domainradar.models.dns.DNSData;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;
import java.util.Set;

/**
 * A record representing a result of the DNS collector.
 *
 * @param dnsData The collected DNS data.
 * @param ips     A set of IP addresses that were found in the A/AAAA records or resolved from the CNAME record.
 * @author Ondřej Ondryáš
 */
public record DNSResult(int statusCode,
                        @Nullable String error,
                        @NotNull Instant lastAttempt,
                        @Nullable DNSData dnsData,
                        @Nullable Set<IPFromRecord> ips
) implements Result {
    public record IPFromRecord(@NotNull String ip, @NotNull String rrType) {
    }
}
