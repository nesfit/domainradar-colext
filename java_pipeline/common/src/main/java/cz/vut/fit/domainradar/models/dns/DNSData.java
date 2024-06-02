package cz.vut.fit.domainradar.models.dns;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Set;

public record DNSData(@NotNull Map<String, Long> ttlValues,
                      @Nullable Set<String> A,
                      @Nullable Set<String> AAAA,
                      @Nullable CNAMERecord CNAME,
                      @Nullable List<MXRecord> MX,
                      @Nullable List<NSRecord> NS,
                      @Nullable List<String> TXT,
                      @Nullable Map<String, String> errors) {
}
