package cz.vut.fit.domainradar.models.requests;

import org.jetbrains.annotations.Nullable;

import java.util.List;

public record IPProcessRequest(
        @Nullable
        List<String> collectors
) {
}
