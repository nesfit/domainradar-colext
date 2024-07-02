package cz.vut.fit.domainradar.models.dns;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Set;

public record ZoneInfo(
        @NotNull String zone,
        @NotNull DNSData.SOARecord soa,
        @NotNull String publicSuffix,
        @Nullable Boolean hasDNSKEY,
        @NotNull Set<String> primaryNameserverIPs,
        @NotNull Set<String> secondaryNameservers,
        @NotNull Set<String> secondaryNameserverIPs
) {
}
