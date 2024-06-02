package cz.vut.fit.domainradar.models.dns;

import org.jetbrains.annotations.NotNull;

public record SOARecord(@NotNull String primaryNs, @NotNull String respMailboxDname, @NotNull String serial,
                        long refresh, long retry, long expire, long minTTL) {
}
