package cz.vut.fit.domainradar.models.results;

import org.jetbrains.annotations.Nullable;

import java.time.Instant;

public record CommonIPResult<TData>(boolean success,
                                    String error,
                                    Instant lastAttempt,
                                    String collector,
                                    @Nullable TData data
) implements Result {
    public static <TData> CommonIPResult<TData> ofError(String collector, Exception e) {
        return new CommonIPResult<>(false, e.getMessage(), Instant.now(), collector, null);
    }
}
