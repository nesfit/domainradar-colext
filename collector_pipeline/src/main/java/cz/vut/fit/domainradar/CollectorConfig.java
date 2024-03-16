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
}
