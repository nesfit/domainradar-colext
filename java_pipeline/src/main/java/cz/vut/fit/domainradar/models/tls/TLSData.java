package cz.vut.fit.domainradar.models.tls;

import cz.vut.fit.domainradar.models.results.DNSResult;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;
import java.util.List;

public record TLSData(@NotNull DNSResult.IPFromRecord fromIp,
                      @NotNull String protocol,
                      @NotNull String cipher,
                      @NotNull List<Certificate> certificates) {

    public record Certificate(@NotNull String dn,
                              byte[] derData) {
    }

}
