package cz.vut.fit.domainradar.models.dns;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Set;

public record ZoneInfo(
        @NotNull String zone,
        @NotNull DNSData.SOARecord soa,
        @NotNull String publicSuffix,
        @NotNull String registrySuffix,
        @Nullable Set<String> primaryNameserverIps,
        @Nullable Set<String> secondaryNameservers,
        @Nullable Set<String> secondaryNameserverIps
) {
}
