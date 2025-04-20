package cz.vut.fit.domainradar;

/**
 * The configuration keys, descriptions and default values for the collectors.
 *
 * @author Ondřej Ondryáš
 * @author Matěj Čech
 */
@SuppressWarnings("ALL")
public class CollectorConfig {
    /* --- GEO-ASN collector --- */
    public static final String GEOIP_DIRECTORY_CONFIG = "collectors.geo-asn.dir";
    public static final String GEOIP_DIRECTORY_DOC = "The path of the directory with the GeoIP mmdb files.";
    public static final String GEOIP_DIRECTORY_DEFAULT = "";

    public static final String GEOIP_CITY_DB_NAME_CONFIG = "collectors.geo-asn.citydb";
    public static final String GEOIP_CITY_DB_NAME_DOC = "The name of the GeoIP City database file.";
    public static final String GEOIP_CITY_DB_NAME_DEFAULT = "GeoLite2-City.mmdb";

    public static final String GEOIP_ASN_DB_NAME_CONFIG = "collectors.geo-asn.asndb";
    public static final String GEOIP_ASN_DB_NAME_DOC = "The name of the GeoIP ASN database file.";
    public static final String GEOIP_ASN_DB_NAME_DEFAULT = "GeoLite2-ASN.mmdb";

    /* --- NERD collector --- */
    public static final String NERD_HTTP_TIMEOUT_CONFIG = "collectors.nerd.timeout";
    public static final String NERD_HTTP_TIMEOUT_DOC = "The request timeout to use in the NERD collector (seconds).";
    public static final String NERD_HTTP_TIMEOUT_DEFAULT = "5";

    public static final String NERD_TOKEN_CONFIG = "collectors.nerd.token";
    public static final String NERD_TOKEN_DOC = "The NERD access token.";
    public static final String NERD_TOKEN_DEFAULT = "";

    public static final String NERD_BATCH_SIZE_CONFIG = "collectors.nerd.batch.size";
    public static final String NERD_BATCH_SIZE_DOC = "The number of IPs to process in a single NERD query.";
    public static final String NERD_BATCH_SIZE_DEFAULT = "64";

    /* --- AbuseIPDB collector --- */
    public static final String ABUSEIPDB_HTTP_TIMEOUT_CONFIG = "collectors.abuseipdb.timeout";
    public static final String ABUSEIPDB_HTTP_TIMEOUT_DOC = "The request timeout to use in the AbuseIPDB collector (seconds).";
    public static final String ABUSEIPDB_HTTP_TIMEOUT_DEFAULT = "5";

    public static final String ABUSEIPDB_TOKEN_CONFIG = "collectors.abuseipdb.token";
    public static final String ABUSEIPDB_TOKEN_DOC = "The AbuseIPDB access token.";
    public static final String ABUSEIPDB_TOKEN_DEFAULT = "";

    /* --- VirusTotal collector --- */
    public static final String VIRUSTOTAL_HTTP_TIMEOUT_CONFIG = "collectors.virustotal.timeout";
    public static final String VIRUSTOTAL_HTTP_TIMEOUT_DOC = "The request timeout to use in the VirusTotal collector (seconds).";
    public static final String VIRUSTOTAL_HTTP_TIMEOUT_DEFAULT = "5";

    public static final String VIRUSTOTAL_TOKEN_CONFIG = "collectors.virustotal.token";
    public static final String VIRUSTOTAL_TOKEN_DOC = "The VirusTotal access token.";
    public static final String VIRUSTOTAL_TOKEN_DEFAULT = "";

    /* --- Hybrid-analysis collector --- */
    public static final String HYBRIDANALYSIS_HTTP_TIMEOUT_CONFIG = "collectors.hybridanalysis.timeout";
    public static final String HYBRIDANALYSIS_HTTP_TIMEOUT_DOC = "The request timeout to use in the Hybrid-analysis collector (seconds).";
    public static final String HYBRIDANALYSIS_HTTP_TIMEOUT_DEFAULT = "5";

    public static final String HYBRIDANALYSIS_TOKEN_CONFIG = "collectors.hybridanalysis.token";
    public static final String HYBRIDANALYSIS_TOKEN_DOC = "The Hybrid-analysis access token.";
    public static final String HYBRIDANALYSIS_TOKEN_DEFAULT = "";

    /* --- Cloudflare Radar collector --- */
    public static final String CLOUDFLARERADAR_HTTP_TIMEOUT_CONFIG = "collectors.cloudflareradar.timeout";
    public static final String CLOUDFLARERADAR_HTTP_TIMEOUT_DOC = "The request timeout to use in the Cloudflare Radar collector (seconds).";
    public static final String CLOUDFLARERADAR_HTTP_TIMEOUT_DEFAULT = "5";

    public static final String CLOUDFLARERADAR_TOKEN_CONFIG = "collectors.cloudflareradar.token";
    public static final String CLOUDFLARERADAR_TOKEN_DOC = "The Cloudflare Radar URLScanner access token.";
    public static final String CLOUDFLARERADAR_TOKEN_DEFAULT = "";

    public static final String CLOUDFLARERADAR_ACCOUNTID_CONFIG = "collectors.cloudflareradar.accountid";
    public static final String CLOUDFLARERADAR_ACCOUNTID_DOC = "The Cloudflare Radar account identifier.";

    /* --- Opentip Kaspersky collector --- */
    public static final String OPENTIPKASPERSKY_HTTP_TIMEOUT_CONFIG = "collectors.opentipkaspersky.timeout";
    public static final String OPENTIPKASPERSKY_HTTP_TIMEOUT_DOC = "The request timeout to use in the Opentip Kaspersky collector (seconds).";
    public static final String OPENTIPKASPERSKY_HTTP_TIMEOUT_DEFAULT = "5";

    public static final String OPENTIPKASPERSKY_TOKEN_CONFIG = "collectors.opentipkaspersky.token";
    public static final String OPENTIPKASPERSKY_TOKEN_DOC = "The Opentip Kaspersky access token.";
    public static final String OPENTIPKASPERSKY_TOKEN_DEFAULT = "";

    /* --- TLS collector --- */
    public static final String TLS_TIMEOUT_MS_CONFIG = "collectors.tls.timeout";
    public static final String TLS_TIMEOUT_MS_DOC = "The TLS socket timeout (milliseconds).";
    public static final String TLS_TIMEOUT_MS_DEFAULT = "3000";

    public static final String TLS_MAX_REDIRECTS_CONFIG = "collectors.tls.max.redirects";
    public static final String TLS_MAX_REDIRECTS_DOC = "The maximum number of HTTP redirects to follow.";
    public static final String TLS_MAX_REDIRECTS_DEFAULT = "2";

    /* --- QRadar collector --- */
    public static final String QRADAR_URL_CONFIG = "collectors.qradar.url";
    public static final String QRADAR_URL_DOC = "The QRadar RESTful API base URL.";
    public static final String QRADAR_URL_DEFAULT = "";

    public static final String QRADAR_TOKEN_CONFIG = "collectors.qradar.token";
    public static final String QRADAR_TOKEN_DOC = "The QRadar RESTful API access token.";
    public static final String QRADAR_TOKEN_DEFAULT = "";

    public static final String QRADAR_BATCH_SIZE_CONFIG = "collectors.qradar.batch.size";
    public static final String QRADAR_BATCH_SIZE_DOC = "The number of IPs to process in a single QRadar query.";
    public static final String QRADAR_BATCH_SIZE_DEFAULT = "10";

    public static final String QRADAR_TIMEOUT_MS_CONFIG = "collectors.qradar.timeout";
    public static final String QRADAR_TIMEOUT_MS_DOC = "The QRadar connection/handshake/request timeout (milliseconds).";
    public static final String QRADAR_TIMEOUT_MS_DEFAULT = "5000";

    public static final String QRADAR_ENTRY_CACHE_LIFETIME_S_CONFIG = "collectors.qradar.entry.cache.lifetime";
    public static final String QRADAR_ENTRY_CACHE_LIFETIME_S_DOC = "Source address cache entry lifetime (seconds).";
    public static final String QRADAR_ENTRY_CACHE_LIFETIME_S_DEFAULT = "120";

    public static final String QRADAR_TRUST_ALL_CONFIG = "collectors.qradar.trust.all";
    public static final String QRADAR_TRUST_ALL_DOC = "If true, the HTTP client will trust self-signed and other untrustworthy certificates provided by the QRadar API.";
    public static final String QRADAR_TRUST_ALL_DEFAULT = "false";

    /* --- General settings for the PC-based standalone collectors --- */
    public static final String CLOSE_TIMEOUT_SEC_CONFIG = "collectors.kafka.close.timeout";
    public static final String CLOSE_TIMEOUT_SEC_DOC =
            "The time to wait for a standalone collector producer/consumer to close (seconds).";
    public static final String CLOSE_TIMEOUT_SEC_DEFAULT = "5";

    public static final String MAX_CONCURRENCY_CONFIG = "collectors.parallel.consumer.max.concurrency";
    public static final String MAX_CONCURRENCY_DOC =
            "The maximum number of input entries to be processed by the collector in parallel.";
    public static final String MAX_CONCURRENCY_DEFAULT = "128";

    public static final String COMMIT_INTERVAL_MS_CONFIG = "collectors.parallel.consumer.commit.interval";
    public static final String COMMIT_INTERVAL_MS_DOC =
            "The interval at which to commit the offsets of processed entries (milliseconds).";
    public static final String COMMIT_INTERVAL_MS_DEFAULT = "1000";

    public static final String COMMIT_MODE_CONFIG = "collectors.parallel.consumer.commit.mode";
    public static final String COMMIT_MODE_DOC = "The parallel consumer commit mode. " +
            "One of PERIODIC_CONSUMER_ASYNCHRONOUS / PERIODIC_CONSUMER_SYNC / PERIODIC_TRANSACTIONAL_PRODUCER. " +
            "See https://github.com/confluentinc/parallel-consumer/tree/master?tab=readme-ov-file#commit-mode for more details.";
    public static final String COMMIT_MODE_DEFAULT = "PERIODIC_CONSUMER_ASYNCHRONOUS";
}
