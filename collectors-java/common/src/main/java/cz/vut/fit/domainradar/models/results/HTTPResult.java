package cz.vut.fit.domainradar.models.results;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;

/**
 * A record representing a result of the HTTP collector.
 *
 * @param targetUrl     The full URL of the actual source of the collected HTML data.
 * @param finalHttpCode The final HTTP status code (after redirects, error handling, etc.).
 * @param truncated     True if the HTML response was truncated due to the configured size limits.
 * @param html          The collected HTML data.
 * @author Ondřej Ondryáš
 */

public record HTTPResult(int statusCode,
                         @Nullable String error,
                         @NotNull Instant lastAttempt,
                         @Nullable String targetUrl,
                         int finalHttpCode,
                         boolean truncated,
                         byte @Nullable [] html
) implements Result {
}
