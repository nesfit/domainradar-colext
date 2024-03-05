package cz.vut.fit.domainradar.models.results;


import java.time.Instant;

public record RDAPDomainResult(
        boolean success,
        String error,
        Instant lastAttempt,
        String registrationDate,
        String lastChangedDate
) implements Result {
}
