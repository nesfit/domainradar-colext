package cz.vut.fit.domainradar.serialization;

import java.util.HashMap;
import java.util.Map;

public final class TagRegistry {
    // TODO: Make the mappings configurable
    public static final Map<String, Integer> TAGS = new HashMap<>(Map.ofEntries(
            Map.entry("geo-asn", 10),
            Map.entry("nerd", 20),
            Map.entry("rdap-ip", 30),
            Map.entry("rtt", 40),
            Map.entry("abuseipdb", 50),
            Map.entry("virustotal", 51),
            Map.entry("pulsedive", 52),
            Map.entry("hybrid-analysis", 53),
            Map.entry("greynoise", 54),
            Map.entry("cloudflare-radar", 55),
            Map.entry("opentip-kaspersky", 56),
            Map.entry("threatfox", 57),
            Map.entry("criminalip", 58),
            Map.entry("google-safe-browsing", 59),
            Map.entry("fortiguard", 60),
            Map.entry("project-honeypot", 61)
    ));

    public static final Map<Integer, String> COLLECTOR_NAMES = Map.ofEntries(
            Map.entry(10, "geo-asn"),
            Map.entry(20, "nerd"),
            Map.entry(30, "rdap-ip"),
            Map.entry(40, "rtt"),
            Map.entry(50, "abuseipdb"),
            Map.entry(51, "virustotal"),
            Map.entry(52, "pulsedive"),
            Map.entry(53, "hybrid-analysis"),
            Map.entry(54, "greynoise"),
            Map.entry(55, "cloudflare-radar"),
            Map.entry(56, "opentip-kaspersky"),
            Map.entry(57, "threatfox"),
            Map.entry(58, "criminalip"),
            Map.entry(59, "google-safe-browsing"),
            Map.entry(60, "fortiguard"),
            Map.entry(61, "project-honeypot")
    );

    public static final Map<String, Integer> REP_SYSTEM_DN_TAGS = new HashMap<>(Map.ofEntries(
            Map.entry("virustotal", 10),
            Map.entry("cloudflare-radar", 11),
            Map.entry("opentip-kaspersky", 12),
            Map.entry("threatfox", 13),
            Map.entry("google-safe-browsing", 14),
            Map.entry("fortiguard", 15),
            Map.entry("urlvoid", 16),
            Map.entry("pulsedive", 17)
    ));

    public static final Map<Integer, String> REP_SYSTEM_DN_COLLECTOR_NAMES = Map.ofEntries(
            Map.entry(10, "virustotal"),
            Map.entry(11, "cloudflare-radar"),
            Map.entry(12, "opentip-kaspersky"),
            Map.entry(13, "threatfox"),
            Map.entry(14, "google-safe-browsing"),
            Map.entry(15, "fortiguard"),
            Map.entry(16, "urlvoid"),
            Map.entry(17, "pulsedive")
    );
}
