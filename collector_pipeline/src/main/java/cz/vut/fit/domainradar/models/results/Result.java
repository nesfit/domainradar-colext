package cz.vut.fit.domainradar.models.results;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;

public interface Result {
    boolean success();
    @Nullable
    String error();
    @NotNull
    Instant lastAttempt();
}
