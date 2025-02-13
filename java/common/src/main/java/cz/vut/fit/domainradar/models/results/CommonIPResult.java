package cz.vut.fit.domainradar.models.results;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;

/**
 * A record representing a result of an IP-based collector.
 *
 * @param collector A string identifier of the collector that created this result.
 * @param data      The data that was collected by the collector.
 * @param <TData>   The type of the data that was collected by the collector.
 * @author Ondřej Ondryáš
 */
public record CommonIPResult<TData>(int statusCode,
                                    @Nullable String error,
                                    @NotNull Instant lastAttempt,
                                    @NotNull String collector,
                                    @Nullable TData data
) implements Result {
}
