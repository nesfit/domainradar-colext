package cz.vut.fit.domainradar.models.requests;

import com.fasterxml.jackson.annotation.JsonProperty;
import cz.vut.fit.domainradar.models.dns.ZoneInfo;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.UnknownNullability;

import java.util.List;

public record DNSProcessRequest(
        @NotNull
        List<String> toCollect,
        @UnknownNullability
        ZoneInfo zoneInfo) {
}
