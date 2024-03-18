package cz.vut.fit.domainradar.models.results;


import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;

public record RDAPDomainResult(int statusCode,
                               @Nullable String error,
                               @NotNull Instant lastAttempt,
                               @Nullable String registrationDate,
                               @Nullable String lastChangedDate
) implements Result {
}
