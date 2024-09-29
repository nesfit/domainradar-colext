package cz.vut.fit.domainradar.serialization;

import java.util.Map;

public final class TagRegistry {
    public static final Map<String, Integer> TAGS = Map.of(
            "geo-asn", 10,
            "nerd", 20,
            "rdap-ip", 30,
            "rtt", 40
    );
}
