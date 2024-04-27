package cz.vut.fit.domainradar.models.requests;

import cz.vut.fit.domainradar.models.dns.ZoneInfo;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.UnknownNullability;

import java.util.List;

public record DNSProcessRequest(
        @Nullable
        List<String> typesToCollect,
        @Nullable
        List<String> typesToProcessIPsFrom,
        @UnknownNullability
        ZoneInfo zoneInfo) {
}
