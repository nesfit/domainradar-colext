package cz.vut.fit.domainradar;

public class CollectorConfig {
    /* --- GeoIP collector --- */
    public static final String GEOIP_DIRECTORY_CONFIG = "collectors.geoip.dir";
    public static final String GEOIP_DIRECTORY_DOC = "The path of the directory with the GeoIP mmdb files.";
    public static final String GEOIP_DIRECTORY_DEFAULT = "";

    public static final String GEOIP_CITY_DB_NAME_CONFIG = "collectors.geoip.citydb";
    public static final String GEOIP_CITY_DB_NAME_DOC = "The name of the GeoIP City database.";
    public static final String GEOIP_CITY_DB_NAME_DEFAULT = "GeoLite2-City.mmdb";

    public static final String GEOIP_ASN_DB_NAME_CONFIG = "collectors.geoip.asndb";
    public static final String GEOIP_ASN_DB_NAME_DOC = "The name of the GeoIP ASN database.";
    public static final String GEOIP_ASN_DB_NAME_DEFAULT = "GeoLite2-ASN.mmdb";

    /* --- NERD collector --- */
    public static final String NERD_HTTP_TIMEOUT_CONFIG = "collectors.nerd.timeout";
    public static final String NERD_HTTP_TIMEOUT_DOC = "The request timeout to use in the NERD collector (seconds).";
    public static final String NERD_HTTP_TIMEOUT_DEFAULT = "3";

    public static final String NERD_TOKEN_CONFIG = "collectors.nerd.token";
    public static final String NERD_TOKEN_DOC = "The NERD access token.";
    public static final String NERD_TOKEN_DEFAULT = "";

    public static final String NERD_BATCH_SIZE_CONFIG = "collectors.nerd.batch.size";
    public static final String NERD_BATCH_SIZE_DOC = "The number of IPs to process in a single NERD query.";
    public static final String NERD_BATCH_SIZE_DEFAULT = "64";

    /* --- DNS collector --- */
    public static final String DNS_DEFAULT_RECORD_TYPES_TO_COLLECT_CONFIG = "collectors.dns.record.types.to.scan";
    public static final String DNS_DEFAULT_RECORD_TYPES_TO_COLLECT_DOC = "The DNS record types to scan if none are specified in the request (comma-separated).";
    public static final String DNS_DEFAULT_RECORD_TYPES_TO_COLLECT_DEFAULT = "A,AAAA,CNAME,NS,MX,TXT";

    public static final String DNS_DEFAULT_TYPES_TO_COLLECT_IPS_FROM_CONFIG = "collectors.dns.record.types.to.collect.ips.from";
    public static final String DNS_DEFAULT_TYPES_TO_COLLECT_IPS_FROM_DOC = "The DNS record types to collect additional IP data for if none are specified in the request (comma-separated).";
    public static final String DNS_DEFAULT_TYPES_TO_COLLECT_IPS_FROM_DEFAULT = "A,AAAA,CNAME,MX";

    public static final String DNS_MAIN_RESOLVER_IPS_CONFIG = "collectors.dns.main.resolver.ips";
    public static final String DNS_MAIN_RESOLVER_IPS_DOC = "IP addresses of the DNS resolvers (comma-separated).";
    public static final String DNS_MAIN_RESOLVER_IPS_DEFAULT = "195.113.144.194,193.17.47.1,195.113.144.233,185.43.135.1";

    public static final String DNS_MAIN_RESOLVER_ROUND_ROBIN_CONFIG = "collectors.dns.main.resolver.round.robin";
    public static final String DNS_MAIN_RESOLVER_ROUND_ROBIN_DOC = "If true, queries will be distributed across the configured DNS resolvers. Otherwise, the first server will be used until it is not available.";
    public static final String DNS_MAIN_RESOLVER_ROUND_ROBIN_DEFAULT = "true";

    public static final String DNS_MAIN_RESOLVER_RANDOMIZE_CONFIG = "collectors.dns.main.resolver.randomize";
    public static final String DNS_MAIN_RESOLVER_RANDOMIZE_DOC = "If true, the order of the DNS resolver IPs will be randomized for each scanner worker.";
    public static final String DNS_MAIN_RESOLVER_RANDOMIZE_DEFAULT = "true";

    public static final String DNS_MAIN_RESOLVER_TIMEOUT_PER_NS_MS_CONFIG = "collectors.dns.main.resolver.timeout.per.ns";
    public static final String DNS_MAIN_RESOLVER_TIMEOUT_PER_NS_MS_DOC = "The timeout for DNS queries made against one of the configured resolvers (milliseconds).";
    public static final String DNS_MAIN_RESOLVER_TIMEOUT_PER_NS_MS_DEFAULT = "4000";

    public static final String DNS_MAIN_RESOLVER_RETRIES_CONFIG = "collectors.dns.main.resolver.retries";
    public static final String DNS_MAIN_RESOLVER_RETRIES_DOC = "The number of attempts to query a single DNS server until the next one is used.";
    public static final String DNS_MAIN_RESOLVER_RETRIES_DEFAULT = "1";

    public static final String DNS_WORKERS_CONFIG = "collectors.dns.workers";
    public static final String DNS_WORKERS_DOC = "The number of workers used to query the DNS for individual records.";
    public static final String DNS_WORKERS_DEFAULT = "256";

    public static final String DNS_TIMEOUT_PER_NS_MS_CONFIG = "collectors.dns.timeout.per.ns";
    public static final String DNS_TIMEOUT_PER_NS_MS_DOC = "The timeout for a single DNS query performed in one attempt against one related nameserver (milliseconds).";
    public static final String DNS_TIMEOUT_PER_NS_MS_DEFAULT = "4000";

    public static final String DNS_RETRIES_PER_NS_CONFIG = "collectors.dns.retries.per.ns";
    public static final String DNS_RETRIES_PER_NS_DOC = "The number of attempts to query a single DNS server until the next one is used.";
    public static final String DNS_RETRIES_PER_NS_DEFAULT = "1";

    public static final String DNS_MAX_TIME_PER_DOMAIN_MS_CONFIG = "collectors.dns.max.time.per.domain";
    public static final String DNS_MAX_TIME_PER_DOMAIN_MS_DOC = "The maximum time that it can take to finish the entire DNS scanning process for a single domain name (milliseconds).";
    public static final String DNS_MAX_TIME_PER_DOMAIN_MS_DEFAULT = "32000";

    public static final String DNS_MAX_TIME_PER_RECORD_MS_CONFIG = "collectors.dns.max.time.per.record";
    public static final String DNS_MAX_TIME_PER_RECORD_MS_DOC = "The maximum time to wait for a single record query attempt (milliseconds).";
    public static final String DNS_MAX_TIME_PER_RECORD_MS_DEFAULT = "16000";

    /* --- Zone collector --- */
    public static final String ZONE_RESOLUTION_TIMEOUT_MS_CONFIG = "collectors.zone.resolution.timeout";
    public static final String ZONE_RESOLUTION_TIMEOUT_MS_DOC = "The time after which a zone resolution request is considered stalled (milliseconds).";
    public static final String ZONE_RESOLUTION_TIMEOUT_MS_DEFAULT = "30000";

    /* --- TLS collector --- */
    public static final String TLS_TIMEOUT_MS_CONFIG = "collectors.tls.timeout";
    public static final String TLS_TIMEOUT_MS_DOC = "The TLS socket timeout (milliseconds).";
    public static final String TLS_TIMEOUT_MS_DEFAULT = "10000";

    /* --- General settings for the standalone collectors --- */
    public static final String CLOSE_TIMEOUT_SEC_CONFIG = "collectors.kafka.close.timeout";
    public static final String CLOSE_TIMEOUT_SEC_DOC = "The time to wait for a standalone collector producer/consumer to close (seconds).";
    public static final String CLOSE_TIMEOUT_SEC_DEFAULT = "5";

    public static final String MAX_CONCURRENCY_CONFIG = "collectors.parallel.consumer.max.concurrency";
    public static final String MAX_CONCURRENCY_DOC = "The maximum number of input entries to be processed by the collector in parallel.";
    public static final String MAX_CONCURRENCY_DEFAULT = "128";

    public static final String COMMIT_INTERVAL_MS_CONFIG = "collectors.parallel.consumer.commit.interval";
    public static final String COMMIT_INTERVAL_MS_DOC = "The interval at which to commit the offsets of processed entries (milliseconds).";
    public static final String COMMIT_INTERVAL_MS_DEFAULT = "100";
}
