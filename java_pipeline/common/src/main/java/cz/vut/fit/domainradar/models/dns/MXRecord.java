package cz.vut.fit.domainradar.models.dns;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;

public record MXRecord(@NotNull String value, int priority, @Nullable List<String> relatedIps) {
}
