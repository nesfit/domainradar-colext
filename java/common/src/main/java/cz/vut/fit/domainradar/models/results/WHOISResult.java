package cz.vut.fit.domainradar.models.results;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;

/**
 * A record representing a result of the WHOIS domain collector.
 *
 * @param whoisTarget The domain name that was used for the actual WHOIS query.
 * @param whoisRaw    The raw WHOIS response data.
 */
public record WHOISResult(int statusCode,
                          @Nullable String error,
                          @NotNull Instant lastAttempt,
                          @NotNull String whoisTarget,
                          @Nullable String whoisRaw
) implements Result {
}
