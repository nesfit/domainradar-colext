package cz.vut.fit.domainradar.standalone.collectors.dns;

import cz.vut.fit.domainradar.models.dns.DNSData;
import org.jetbrains.annotations.Nullable;

import java.util.*;

class DNSDataContainer {
    DNSDataContainer(int mask, List<String> typesToProcessIPsFrom) {
        this.missingMask = this.initialMask = mask;
        this.typesToProcessIPsFrom = typesToProcessIPsFrom;
        this.recordCollectionErrors = new HashMap<>();
    }

    @Nullable Set<String> A;
    @Nullable Set<String> AAAA;
    @Nullable DNSData.CNAMERecord CNAME;
    @Nullable List<DNSData.MXRecord> MX;
    @Nullable List<DNSData.NSRecord> NS;
    @Nullable List<String> TXT;
    long ttlA = -1, ttlAAAA = -1, ttlCNAME = -1, ttlMX = -1, ttlNS = -1, ttlTXT = -1;
    int missingMask, initialMask;
    List<String> typesToProcessIPsFrom;
    Map<String, String> recordCollectionErrors;
    TimerTask clearStalledTask;
}
