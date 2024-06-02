package cz.vut.fit.domainradar.models.results;


import com.fasterxml.jackson.databind.JsonNode;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;

public record RDAPDomainResult(int statusCode,
                               @Nullable String error,
                               @NotNull Instant lastAttempt,
                               @Nullable String rdapData, // Raw JSON
                               @Nullable String entities, // Raw JSON
                               boolean forSourceName,
                               int whoisStatusCode,
                               @Nullable String whoisError,
                               @Nullable String whoisRaw,
                               @Nullable String whoisParsed
) implements Result {
}
