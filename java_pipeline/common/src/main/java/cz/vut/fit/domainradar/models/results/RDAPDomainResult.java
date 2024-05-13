package cz.vut.fit.domainradar.models.results;


import com.fasterxml.jackson.databind.JsonNode;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;

public record RDAPDomainResult(int statusCode,
                               @Nullable String error,
                               @NotNull Instant lastAttempt,
                               @Nullable JsonNode rdapData,
                               @Nullable JsonNode entities,
                               boolean forSourceName,
                               int whoisStatusCode,
                               @Nullable String whoisError,
                               @Nullable String whoisRaw,
                               @Nullable JsonNode whoisParsed
) implements Result {
}
