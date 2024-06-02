package cz.vut.fit.domainradar.models.results;

import cz.vut.fit.domainradar.models.dns.DNSData;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;
import java.util.Set;

public record DNSResult(int statusCode,
                        @Nullable String error,
                        @NotNull Instant lastAttempt,
                        @Nullable DNSData dnsData,
                        @Nullable Set<IPFromRecord> ips
) implements Result {
}
