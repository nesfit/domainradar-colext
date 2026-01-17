package cz.vut.fit.domainradar.models.results;

import cz.vut.fit.domainradar.models.dns.ZoneInfo;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;

/**
 * A record representing a result of the zone collector.
 *
 * @param zone The collected zone data.
 */
public record ZoneResult(int statusCode,
                         @Nullable String error,
                         @NotNull Instant lastAttempt,
                         @Nullable ZoneInfo zone
) implements Result {
}
