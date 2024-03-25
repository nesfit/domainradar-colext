package cz.vut.fit.domainradar.models.requests;

import org.jetbrains.annotations.Nullable;

import java.util.List;

public record ZoneProcessRequest(
        @Nullable
        List<String> toCollect,
        @Nullable
        List<String> typesToProcessIPsFrom) {

    public ZoneProcessRequest() {
        this(null, null);
    }
}
