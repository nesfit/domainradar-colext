package cz.vut.fit.domainradar.models.results;

import java.time.Instant;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import cz.vut.fit.domainradar.models.tls.TLSData;
/**
 * A record representing a result of the TLS collector.
 *
 * @param tlsData The collected TLS data.
 * @param html The collected HTML data.
 * @author Ondřej Ondryáš
 */

public record TLSResult(int statusCode,
                        @Nullable String error,
                        @NotNull Instant lastAttempt,
                        @Nullable TLSData tlsData,
                        @Nullable String html
                        
) implements Result {
}
