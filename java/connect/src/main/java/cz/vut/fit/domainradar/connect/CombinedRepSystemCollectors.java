package cz.vut.fit.domainradar.connect;

import java.util.Set;

/**
 * Set of collector names that collect data for both IP addresses and domain names.
 *
 * @author Matěj Čech
 */
public final class CombinedRepSystemCollectors {
    public static final Set<String> COMBINED_REP_SYSTEM_COLLECTORS = Set.of(
            "virustotal",
            "pulsedive",
            "hybrid-analysis",
            "cloudflare-radar",
            "opentip-kaspersky",
            "threatfox",
            "google-safe-browsing",
            "fortiguard"
    );
}
