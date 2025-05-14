package cz.vut.fit.domainradar.standalone.collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.net.InetAddresses;
import cz.vut.fit.domainradar.CollectorConfig;
import cz.vut.fit.domainradar.Common;
import cz.vut.fit.domainradar.models.DNToProcess;
import cz.vut.fit.domainradar.models.IPToProcess;
import cz.vut.fit.domainradar.models.repsystems.GoogleSafeBrowsingData;
import cz.vut.fit.domainradar.standalone.BaseCombinedRepSystemAPICollector;
import org.json.JSONObject;

import java.net.Inet6Address;
import java.util.Properties;

/**
 * A collector that processes IP and DN data using the Google Safe Browsing API.
 *
 * @author Matěj Čech
 */
public class GoogleSafeBrowsingCollector extends BaseCombinedRepSystemAPICollector<GoogleSafeBrowsingData> {
    public static final String NAME = "google-safe-browsing";
    public static final String COMPONENT_NAME = "collector-" + NAME;
    private static final org.slf4j.Logger Logger = Common.getComponentLogger(GoogleSafeBrowsingCollector.class);

    private static String GOOGLE_SAFE_BROWSING_BASE =
            "https://safebrowsing.googleapis.com/v4/threatMatches:find?key=";

    public GoogleSafeBrowsingCollector(ObjectMapper jsonMapper, String appName, Properties properties) {
        super(jsonMapper, appName, properties, properties.getProperty(CollectorConfig.GOOGLESAFEBROWSING_TOKEN_CONFIG,
                        CollectorConfig.GOOGLESAFEBROWSING_TOKEN_DEFAULT), CollectorConfig.GOOGLESAFEBROWSING_HTTP_TIMEOUT_CONFIG,
                CollectorConfig.GOOGLESAFEBROWSING_HTTP_TIMEOUT_DEFAULT);

        GOOGLE_SAFE_BROWSING_BASE += properties.getProperty(CollectorConfig.GOOGLESAFEBROWSING_TOKEN_CONFIG,
                CollectorConfig.GOOGLESAFEBROWSING_TOKEN_DEFAULT);
    }

    @Override
    protected org.slf4j.Logger getLogger() {
        return Logger;
    }

    @Override
    protected String getRequestUrl(IPToProcess ip) {
        final var ipInet = InetAddresses.forString(ip.ip());

        // IPv6 is not supported by Google Safe Browsing
        if (ipInet instanceof Inet6Address) {
            return null;
        }

        return GOOGLE_SAFE_BROWSING_BASE;
    }

    @Override
    protected String getRequestUrl(DNToProcess dn) {
        return GOOGLE_SAFE_BROWSING_BASE;
    }

    @Override
    protected String getAuthTokenHeaderName() {
        return null;
    }

    @Override
    protected String getCollectorName() {
        return NAME;
    }

    @Override
    protected GoogleSafeBrowsingData mapIPResponseToData(JSONObject jsonResponse) {
        return mapResponseData(jsonResponse);
    }

    @Override
    protected GoogleSafeBrowsingData mapDNResponseToData(JSONObject jsonResponse) {
        return mapResponseData(jsonResponse);
    }

    private GoogleSafeBrowsingData mapResponseData(JSONObject jsonResponse) {
        Integer unspecified_cnt = 0;
        Integer malware_cnt = 0;
        Integer social_engineering_cnt = 0;
        Integer unwanted_software_cnt = 0;
        Integer potentially_harmful_cnt = 0;

        if (jsonResponse.has("matches")) {
            var matches = jsonResponse.getJSONArray("matches");

            for (int i = 0; i < matches.length(); i++) {
                var threatType = matches.getJSONObject(i).getString("threatType");

                switch (threatType) {
                    case "THREAT_TYPE_UNSPECIFIED":
                        unspecified_cnt++;
                        break;
                    case "MALWARE":
                        malware_cnt++;
                        break;
                    case "SOCIAL_ENGINEERING":
                        social_engineering_cnt++;
                        break;
                    case "UNWANTED_SOFTWARE":
                        unwanted_software_cnt++;
                        break;
                    case "POTENTIALLY_HARMFUL_APPLICATION":
                        potentially_harmful_cnt++;
                        break;
                }
            }
        }

        return new GoogleSafeBrowsingData(
                unspecified_cnt,
                malware_cnt,
                social_engineering_cnt,
                unwanted_software_cnt,
                potentially_harmful_cnt
        );
    }

    @Override
    protected String getPOSTData(IPToProcess ip) {
        return getPOSTData(ip.ip());
    }

    @Override
    protected String getPOSTData(DNToProcess dn) {
        return getPOSTData(dn.dn());
    }

    private String getPOSTData(String target) {
        return "{"
                + "\"client\": {"
                + "\"clientId\": \"domrad-bp\","
                + "\"clientVersion\": \"1.0\""
                + "},"
                + "\"threatInfo\": {"
                + "\"threatTypes\": [\"THREAT_TYPE_UNSPECIFIED\", \"MALWARE\", \"SOCIAL_ENGINEERING\", \"UNWANTED_SOFTWARE\", \"POTENTIALLY_HARMFUL_APPLICATION\"],"
                + "\"platformTypes\": [\"ALL_PLATFORMS\"],"
                + "\"threatEntryTypes\": [\"URL\"],"
                + "\"threatEntries\": ["
                + "{ \"url\": \"" + target + "\" }"
                + "]"
                + "}"
                + "}";
    }
}
