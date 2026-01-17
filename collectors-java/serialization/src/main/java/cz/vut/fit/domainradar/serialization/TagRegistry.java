package cz.vut.fit.domainradar.serialization;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class TagRegistry {
    // TODO: Make the mappings configurable
    public static final Map<String, Integer> TAGS = Map.of(
            "geo-asn", 10,
            "nerd", 20,
            "rdap-ip", 30,
            "rtt", 40,
            "qradar", 120
    );

    public static final Map<Integer, String> COLLECTOR_NAMES = Map.of(
            10, "geo-asn",
            20, "nerd",
            30, "rdap-ip",
            40, "rtt",
            120, "qradar"
    );

    public static final List<Integer> TAGS_TO_INCLUDE_IN_MERGED_RESULT = new ArrayList<>(List.of(
            10, // geo-asn
            20, // nerd
            30, // rdap-ip
            40  // rtt
    ));
}
