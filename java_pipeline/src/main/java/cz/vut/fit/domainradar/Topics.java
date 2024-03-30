package cz.vut.fit.domainradar;

public final class Topics {
    public static final String IN_ZONE = "to_process_zone";
    public static final String IN_DNS = "to_process_DNS";
    public static final String IN_RDAP_DN = "to_process_RDAP_DN";
    public static final String IN_IP = "to_process_IP";

    public static final String OUT_ZONE = "processed_zone";
    public static final String OUT_DNS = "processed_DNS";
    public static final String OUT_RDAP_DN = "processed_RDAP_DN";
    public static final String OUT_IP = "collected_IP_data";

    public static final String OUT_MERGE_DNS_IP = "merged_DNS_IP";
    public static final String OUT_MERGE_ALL = "all_collected_data";
}
