package cz.vut.fit.domainradar.standalone.collectors.dns;

import cz.vut.fit.domainradar.models.dns.CNAMERecord;
import cz.vut.fit.domainradar.models.dns.MXRecord;
import cz.vut.fit.domainradar.models.dns.NSRecord;
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
    @Nullable CNAMERecord CNAME;
    @Nullable List<MXRecord> MX;
    @Nullable List<NSRecord> NS;
    @Nullable List<String> TXT;
    long ttlA = -1, ttlAAAA = -1, ttlCNAME = -1, ttlMX = -1, ttlNS = -1, ttlTXT = -1;
    int missingMask, initialMask;
    List<String> typesToProcessIPsFrom;
    Map<String, String> recordCollectionErrors;
    TimerTask clearStalledTask;
}
