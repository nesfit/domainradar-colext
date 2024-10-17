package cz.vut.fit.domainradar;

/**
 * The configuration keys, descriptions and default values for the Flink-based merger.
 *
 * @author Ondřej Ondryáš
 */
@SuppressWarnings("ALL")
public class MergerConfig {
    /* --- Domain-based collector results merger --- */
    /**
     * The time (in ms) to wait for potential updates to a domain-data aggregate entry that has already been
     * finalized. Setting this to a higher value increases per-entry latency.
     */
    public static final String DN_FINISHED_ENTRY_GRACE_PERIOD_MS_CONFIG = "dn.finished.entry.grace.period.ms";
    public static final String DN_FINISHED_ENTRY_GRACE_PERIOD_DEFAULT = "5000"; // 4 seconds

    /**
     * The time (in ms) to wait after the last update to a domain-data aggregate entry before it is produced
     * even if it is not finished. This should be set to a value higher that the maximum expected time before
     * a collector response; however, setting it too high will cause large amount of retained state data.
     */
    public static final String DN_MAX_ENTRY_LIFETIME_MS_CONFIG = "dn.max.entry.lifetime.ms";
    public static final String DN_MAX_ENTRY_LIFETIME_DEFAULT = "60000"; // 1 minute

    /* --- IP-based collector results merger --- */
    /**
     * The time (in ms) to wait for potential updates to a final merged data entry that has already been finalized.
     * Setting this to a higher value increases the per-domain latency.
     */
    public static final String IP_FINISHED_ENTRY_GRACE_PERIOD_MS_CONFIG = "ip.finished.entry.grace.period.ms";
    public static final String IP_FINISHED_ENTRY_GRACE_PERIOD_DEFAULT = "5000"; // 4 seconds

    /**
     * The time (in ms) to wait after the last domain-data aggregate update before an unfinished final merged data entry
     * is produced. This should be set to a value higher that the maximum expected time between the last DN-based
     * collector result and the first IP-based collector result for the same domain; however, setting it too high will
     * increase the per-domain latency and can cause large amount of retained state data.
     */
    public static final String IP_MAX_ENTRY_LIFETIME_AFTER_DOMAIN_DATA_MS_CONFIG
            = "ip.max.entry.lifetime.after.domain.data.ms";
    public static final String IP_MAX_ENTRY_LIFETIME_AFTER_DOMAIN_DATA_DEFAULT = "10000"; // 10 seconds

    /**
     * The time (in ms) to wait after the last IP-based collector result before an unfinished final merged data entry
     * is produced. This should be set to a value higher that the maximum expected time between the first and the
     * last IP-based collector result for a single IP; however, setting it too high will increase the per-domain latency
     * and can cause large amount of retained state data.
     */
    public static final String IP_MAX_ENTRY_LIFETIME_AFTER_IP_DATA_MS_CONFIG
            = "ip.max.entry.lifetime.after.ip.data.ms";
    public static final String IP_MAX_ENTRY_LIFETIME_AFTER_IP_DATA_DEFAULT = "60000"; // 1 minute
}
