package cz.vut.fit.domainradar.models.results;

import com.fasterxml.jackson.databind.JsonNode;
import cz.vut.fit.domainradar.models.ResultCodes;
import cz.vut.fit.domainradar.models.dns.DNSData;
import cz.vut.fit.domainradar.models.tls.TLSData;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;
import java.util.Map;
import java.util.Set;

/**
 * A result of the DNS/TLS collector merged with results from the IP collectors.
 *
 * @param statusCode  A status code.
 * @param error       An error message.
 * @param lastAttempt A timestamp of the DNS collection process end.
 * @param dnsData     A container of DNS results.
 * @param tlsData     A container of TLS results.
 * @param ips         A set of IP addresses coupled with the name of the DNS record type the address was extracted from.
 * @param ipResults   A map of [IP Address] -> (map of [Collector ID] -> [Result]).
 * @see Result
 * @see ResultCodes
 */
public record ExtendedDNSResult(int statusCode,
                                @Nullable String error,
                                @NotNull Instant lastAttempt,
                                @Nullable DNSData dnsData,
                                @Nullable TLSData tlsData,
                                @Nullable Set<DNSResult.IPFromRecord> ips,
                                @Nullable Map<String, Map<String, CommonIPResult<JsonNode>>> ipResults
) implements Result {
    /**
     * Makes an instance by copying fields from the given <code>{@link DNSResult}</code> and storing the
     * given IP data map as <code>{@link #ipResults}</code>. This makes a shallow copy of the input DNS result
     * and stores a reference to the map, not a copy.
     */
    public ExtendedDNSResult(DNSResult result, @Nullable Map<String, Map<String, CommonIPResult<JsonNode>>> ips) {
        this(result.statusCode(), result.error(), result.lastAttempt(), result.dnsData(), result.tlsData(),
                result.ips(), ips);
    }
}
