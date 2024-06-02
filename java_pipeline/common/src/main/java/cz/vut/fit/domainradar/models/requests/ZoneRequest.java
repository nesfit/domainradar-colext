package cz.vut.fit.domainradar.models.requests;

import org.jetbrains.annotations.Nullable;

import java.util.List;

public record ZoneRequest(
        boolean collectDNS,
        boolean collectRDAP,
        @Nullable
        List<String> dnsTypesToCollect,
        @Nullable
        List<String> dnsTypesToProcessIPsFrom) {

    public ZoneRequest() {
        this(true, true, null, null);
    }
}
