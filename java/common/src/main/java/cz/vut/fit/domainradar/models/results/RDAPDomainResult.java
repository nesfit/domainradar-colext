package cz.vut.fit.domainradar.models.results;

import com.fasterxml.jackson.databind.JsonNode;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;

/**
 * A record representing a result of the RDAP domain collector.
 *
 * @param rdapTarget      The domain name that was used for the actual RDAP query.
 * @param rdapData        The raw RDAP data.
 * @param entities        The entities extracted from the RDAP data.
 */
public record RDAPDomainResult(int statusCode,
                               @Nullable String error,
                               @NotNull Instant lastAttempt,
                               @NotNull String rdapTarget,
                               @Nullable JsonNode rdapData,
                               @Nullable JsonNode entities
) implements Result {
}
