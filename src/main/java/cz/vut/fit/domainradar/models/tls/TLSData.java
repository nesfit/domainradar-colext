package cz.vut.fit.domainradar.models.tls;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;
import java.util.List;

public record TLSData(@NotNull String protocol,
                      @NotNull String cipher,
                      int count,
                      @NotNull List<Certificate> certificates) {
    public record CertificateExtension(boolean critical,
                                       @NotNull String name,
                                       @NotNull String value) {
    }

    public record Certificate(@NotNull String commonName,
                              @Nullable String country,
                              boolean isRoot,
                              @Nullable String organization,
                              @NotNull Integer validLen,
                              @NotNull Instant validityEnd,
                              @NotNull Instant validityStart,
                              int extensionCount,
                              @Nullable List<CertificateExtension> extensions) {
    }

}
