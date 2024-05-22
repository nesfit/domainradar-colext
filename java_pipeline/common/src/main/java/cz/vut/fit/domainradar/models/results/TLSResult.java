package cz.vut.fit.domainradar.models.results;

import cz.vut.fit.domainradar.models.tls.TLSData;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;

public record TLSResult(int statusCode,
                        @Nullable String error,
                        @NotNull Instant lastAttempt,
                        @Nullable TLSData tlsData
) implements Result {
}
