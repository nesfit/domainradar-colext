package cz.vut.fit.domainradar.models.results;


import com.fasterxml.jackson.databind.JsonNode;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;

/**
 * A record representing a result of an RDAP domain collector.
 *
 * @param rdapTarget      The domain name at which the RDAP query was targeted.
 * @param rdapData        The raw RDAP data.
 * @param entities        The entities extracted from the RDAP data.
 * @param whoisStatusCode The status code of the WHOIS query.
 * @param whoisError      The error message of the WHOIS query.
 * @param whoisRaw        The raw WHOIS data.
 * @param whoisParsed     The parsed WHOIS data.
 */
public record RDAPDomainResult(int statusCode,
                               @Nullable String error,
                               @NotNull Instant lastAttempt,
                               @NotNull String rdapTarget,
                               @Nullable JsonNode rdapData,
                               @Nullable JsonNode entities,
                               int whoisStatusCode,
                               @Nullable String whoisError,
                               // The spec defines whoisRaw as str | list[str]
                               // Unless the Java components actively work with this type, let's just keep
                               // it as a JsonNode
                               @Nullable JsonNode whoisRaw,
                               @Nullable JsonNode whoisParsed
) implements Result {
}
