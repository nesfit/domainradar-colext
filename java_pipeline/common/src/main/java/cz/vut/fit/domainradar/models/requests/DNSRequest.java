package cz.vut.fit.domainradar.models.requests;

import com.fasterxml.jackson.annotation.JsonProperty;
import cz.vut.fit.domainradar.models.dns.ZoneInfo;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.UnknownNullability;

import java.util.List;

public record DNSRequest(
        @Nullable
        List<String> typesToCollect,
        @Nullable
        List<String> typesToProcessIPsFrom,
        @NotNull
        @JsonProperty(required = true)
        ZoneInfo zoneInfo) {
}
