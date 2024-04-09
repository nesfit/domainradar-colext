package cz.vut.fit.domainradar;

public class CollectorConfig {
    public static final String GEOIP_DIRECTORY_CONFIG = "collectors.geoip.dir";
    public static final String GEOIP_DIRECTORY_DOC = "The path of the directory with the GeoIP mmdb files.";

    public static final String GEOIP_CITY_DB_NAME_CONFIG = "collectors.geoip.citydb";
    public static final String GEOIP_CITY_DB_NAME_DOC = "The name of the GeoIP City database.";
    public static final String GEOIP_CITY_DB_NAME_DEFAULT = "GeoLite2-City.mmdb";


    public static final String GEOIP_ASN_DB_NAME_CONFIG = "collectors.geoip.asndb";
    public static final String GEOIP_ASN_DB_NAME_DOC = "The name of the GeoIP ASN database.";
    public static final String GEOIP_ASN_DB_NAME_DEFAULT = "GeoLite2-ASN.mmdb";

    public static final String NERD_HTTP_TIMEOUT_CONFIG = "collectors.nerd.timeout";
    public static final String NERD_HTTP_TIMEOUT_DOC = "The request timeout to use in the NERD collector (seconds).";
    public static final String NERD_HTTP_TIMEOUT_DEFAULT = "3";

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
    public static final String DNS_MAIN_RESOLVER_ROUND_ROBIN_DEFAULT = "false";

    public static final String DNS_MAIN_RESOLVER_TIMEOUT_SEC_CONFIG = "collectors.dns.main.resolver.timeout";
    public static final String DNS_MAIN_RESOLVER_TIMEOUT_SEC_DOC = "The timeout for a DNS query when doing the  (seconds).";
    public static final String DNS_MAIN_RESOLVER_TIMEOUT_SEC_DEFAULT = "5";

    public static final String DNS_MAIN_RESOLVER_RETRIES_CONFIG = "collectors.dns.main.resolver.retries";
    public static final String DNS_MAIN_RESOLVER_RETRIES_DOC = "The number of attempts to query a single DNS server until the next one is used.";
    public static final String DNS_MAIN_RESOLVER_RETRIES_DEFAULT = "1";


    public static final String DNS_PER_DOMAIN_RESOLVER_TIMEOUT_SEC_CONFIG = "collectors.dns.per.domain.resolver.timeout";
    public static final String DNS_PER_DOMAIN_RESOLVER_TIMEOUT_SEC_DOC = "The timeout for a DNS query (seconds).";
    public static final String DNS_PER_DOMAIN_RESOLVER_TIMEOUT_SEC_DEFAULT = "5";

    public static final String DNS_PER_DOMAIN_RESOLVER_RETRIES_CONFIG = "collectors.dns.per.domain.resolver.retries";
    public static final String DNS_PER_DOMAIN_RESOLVER_RETRIES_DOC = "The number of attempts to query a single DNS server until the next one is used.";
    public static final String DNS_PER_DOMAIN_RESOLVER_RETRIES_DEFAULT = "1";


    public static final String CLOSE_TIMEOUT_SEC_CONFIG = "collectors.kafka.close.timeout";
    public static final String CLOSE_TIMEOUT_SEC_DOC = "The time to wait for a standalone collector producer/consumer to close (seconds).";
    public static final String CLOSE_TIMEOUT_SEC_DEFAULT = "5";

    public static final String MAX_CONCURRENCY_CONFIG = "collectors.parallel.consumer.max.concurrency";
    public static final String MAX_CONCURRENCY_DOC = "The maximum number of concurrent requests to make.";
    public static final String MAX_CONCURRENCY_DEFAULT = "2";

    public static final String COMMIT_INTERVAL_MS_CONFIG = "collectors.parallel.consumer.commit.interval";
    public static final String COMMIT_INTERVAL_MS_DOC = "The interval at which to commit the offsets.";
    public static final String COMMIT_INTERVAL_MS_DEFAULT = "100";

}
