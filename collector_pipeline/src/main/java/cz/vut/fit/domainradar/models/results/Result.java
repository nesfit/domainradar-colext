package cz.vut.fit.domainradar.models.results;

import cz.vut.fit.domainradar.pipeline.ErrorCodes;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;

public interface Result {
    int statusCode();
    @Nullable
    String error();
    @NotNull
    Instant lastAttempt();

    default boolean success() {
        return this.statusCode() == ErrorCodes.OK;
    }
}
