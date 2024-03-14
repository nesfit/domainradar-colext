package cz.vut.fit.domainradar.models.requests;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.jetbrains.annotations.Nullable;

import java.util.List;

public record ZoneProcessRequest(
        @JsonProperty
        @Nullable
        List<String> toCollect) {

    public ZoneProcessRequest() {
        this(null);
    }
}
