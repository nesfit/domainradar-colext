package cz.vut.fit.domainradar.standalone.collectors.dns;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public record ProcessedItem(
        @NotNull String domainName,

        @NotNull String recordType,
        @Nullable Object value,
        long ttl,
        @Nullable String error) {
}
