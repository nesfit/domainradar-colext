package cz.vut.fit.domainradar.models.results;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;

public record CommonIPResult<TData>(int statusCode,
                                    @Nullable String error,
                                    @NotNull Instant lastAttempt,
                                    @NotNull String collector,
                                    @Nullable TData data
) implements Result {
}
