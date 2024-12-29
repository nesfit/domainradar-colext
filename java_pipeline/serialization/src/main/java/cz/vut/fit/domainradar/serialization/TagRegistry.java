package cz.vut.fit.domainradar.serialization;

import java.util.HashMap;
import java.util.Map;

public final class TagRegistry {
    // TODO: Make the mappings configurable
    public static final Map<String, Integer> TAGS = new HashMap<>(Map.of(
            "geo-asn", 10,
            "nerd", 20,
            "rdap-ip", 30,
            "rtt", 40
    ));

    public static final Map<Integer, String> COLLECTOR_NAMES = Map.of(
            10, "geo-asn",
            20, "nerd",
            30, "rdap-ip",
            40, "rtt"
    );
}
