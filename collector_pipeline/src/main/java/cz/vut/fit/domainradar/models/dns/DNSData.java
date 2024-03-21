package cz.vut.fit.domainradar.models.dns;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.UnknownNullability;

import java.util.List;
import java.util.Map;

public record DNSData(@NotNull Map<String, Integer> ttlValues,
                      @Nullable SOARecord SOA,
                      @Nullable NSRecord NS,
                      @Nullable List<String> A,
                      @Nullable List<String> AAAA,
                      @Nullable CNAMERecord CNAME,
                      @Nullable List<MXRecord> MX,
                      @Nullable List<String> TXT) {
    public record IPRecord(int ttl, @NotNull String value) {
    }

    public record CNAMERecord(@NotNull String value, @Nullable List<IPRecord> relatedIps) {
    }

    public record MXRecord(@NotNull String value, int priority, @Nullable List<IPRecord> relatedIps) {
    }

    public record NSRecord(@NotNull String nameserver, @Nullable List<IPRecord> relatedIps) {
    }

    public record SOARecord(@NotNull String primaryNs, @NotNull String respMailboxDname, @NotNull String serial,
                            long refresh, long retry, long expire, long minTTL) {
    }
}
