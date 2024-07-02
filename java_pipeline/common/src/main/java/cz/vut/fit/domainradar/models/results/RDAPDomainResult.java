package cz.vut.fit.domainradar.models.results;


import com.fasterxml.jackson.databind.JsonNode;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;

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
