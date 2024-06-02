package cz.vut.fit.domainradar.models.tls;

import org.jetbrains.annotations.NotNull;

import java.util.List;

public record TLSData(@NotNull String fromIp,
                      @NotNull String protocol,
                      @NotNull String cipher,
                      @NotNull List<Certificate> certificates) {

}
