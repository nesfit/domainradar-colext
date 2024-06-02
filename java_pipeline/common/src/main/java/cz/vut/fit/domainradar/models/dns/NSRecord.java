package cz.vut.fit.domainradar.models.dns;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;

public record NSRecord(@NotNull String nameserver, @Nullable List<String> relatedIps) {
}
