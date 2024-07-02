package cz.vut.fit.domainradar.models.tls;

import org.jetbrains.annotations.NotNull;

import java.util.List;

public record TLSData(@NotNull String fromIP,
                      @NotNull String protocol,
                      @NotNull String cipher,
                      @NotNull List<Certificate> certificates) {

    public record Certificate(@NotNull String dn,
                              byte[] derData) {
    }

}
