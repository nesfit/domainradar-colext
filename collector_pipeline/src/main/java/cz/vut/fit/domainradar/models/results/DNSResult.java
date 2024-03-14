package cz.vut.fit.domainradar.models.results;

import cz.vut.fit.domainradar.models.dns.DNSData;
import cz.vut.fit.domainradar.models.tls.TLSData;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;
import java.util.Set;

public record DNSResult(boolean success,
                        String error,
                        Instant lastAttempt,
                        @Nullable DNSData dnsData,
                        @Nullable TLSData tlsData,
                        @Nullable Set<String> ips
) implements Result {
}
