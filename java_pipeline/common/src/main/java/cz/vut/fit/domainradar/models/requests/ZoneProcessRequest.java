package cz.vut.fit.domainradar.models.requests;

import org.jetbrains.annotations.Nullable;

import java.util.List;

public record ZoneProcessRequest(
        boolean collectDNS,
        boolean collectRDAP,
        @Nullable
        List<String> dnsTypesToCollect,
        @Nullable
        List<String> dnsTypesToProcessIPsFrom) {

    public ZoneProcessRequest() {
        this(true, true, null, null);
    }
}
