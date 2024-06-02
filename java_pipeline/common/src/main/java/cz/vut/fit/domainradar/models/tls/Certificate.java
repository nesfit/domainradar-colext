package cz.vut.fit.domainradar.models.tls;

import org.jetbrains.annotations.NotNull;

public record Certificate(@NotNull String dn,
                          byte[] derData) {
}
