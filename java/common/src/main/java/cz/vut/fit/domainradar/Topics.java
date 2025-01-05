package cz.vut.fit.domainradar;

import java.util.HashMap;
import java.util.Map;

/**
 * The Kafka topic names.
 *
 * @author Ondřej Ondryáš
 */
public final class Topics {
    public static final String IN_ZONE = "to_process_zone";
    public static final String IN_DNS = "to_process_DNS";
    public static final String IN_RDAP_DN = "to_process_RDAP_DN";
    public static final String IN_IP = "to_process_IP";
    public static final String IN_TLS = "to_process_TLS";

    public static final String OUT_ZONE = "processed_zone";
    public static final String OUT_DNS = "processed_DNS";
    public static final String OUT_RDAP_DN = "processed_RDAP_DN";
    public static final String OUT_TLS = "processed_TLS";
    public static final String OUT_IP = "collected_IP_data";

    public static final String OUT_MERGE_ALL = "all_collected_data";

    /**
     * Mapping of the output topics to the collector IDs.
     */
    public static final Map<String, String> TOPICS_TO_COLLECTOR_ID = new HashMap<>();

    static {
        TOPICS_TO_COLLECTOR_ID.put(OUT_DNS, "dns");
        TOPICS_TO_COLLECTOR_ID.put(OUT_TLS, "tls");
        TOPICS_TO_COLLECTOR_ID.put(OUT_ZONE, "zone");
        TOPICS_TO_COLLECTOR_ID.put(OUT_RDAP_DN, "rdap-dn");
    }
}
