package cz.vut.fit.domainradar.models.ip;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;

public record QRadarData(
        long sourceAddressId,
        long qradarDomainId,
        double magnitude,
        @NotNull List<QRadarOffense> offenses
) {
    public record QRadarOffense(
            long id,
            @Nullable String description,
            long eventCount,
            long flowCount,
            long deviceCount,
            double severity,
            double magnitude,
            long lastUpdatedTime,
            @Nullable String status
    ) {
    }
}
