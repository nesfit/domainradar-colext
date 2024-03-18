package cz.vut.fit.domainradar.models.results;

import com.fasterxml.jackson.databind.JsonNode;
import cz.vut.fit.domainradar.models.dns.DNSData;
import cz.vut.fit.domainradar.models.tls.TLSData;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

public record ExtendedDNSResult(int statusCode,
                                @Nullable String error,
                                @NotNull Instant lastAttempt,
                                @Nullable DNSData dnsData,
                                @Nullable TLSData tlsData,
                                @Nullable Map<String, Map<String, CommonIPResult<JsonNode>>> ips
) implements Result {
    public ExtendedDNSResult(DNSResult result, @Nullable Map<String, Map<String, CommonIPResult<JsonNode>>> ips) {
        this(result.statusCode(), result.error(), result.lastAttempt(), result.dnsData(), result.tlsData(), ips);
    }
}
