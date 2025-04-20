package cz.vut.fit.domainradar.standalone.collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.net.InetAddresses;
import cz.vut.fit.domainradar.CollectorConfig;
import cz.vut.fit.domainradar.Common;
import cz.vut.fit.domainradar.models.DNToProcess;
import cz.vut.fit.domainradar.models.IPToProcess;
import cz.vut.fit.domainradar.models.repsystems.ThreatfoxData;
import cz.vut.fit.domainradar.standalone.BaseCombinedRepSystemAPICollector;
import org.json.JSONObject;

import java.net.Inet6Address;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * A collector that processes IP and DN data using the Threatfox API.
 *
 * @author Matěj Čech
 */
public class ThreatfoxCollector extends BaseCombinedRepSystemAPICollector<ThreatfoxData> {
    public static final String NAME = "threatfox";
    public static final String COMPONENT_NAME = "collector-" + NAME;
    private static final org.slf4j.Logger Logger = Common.getComponentLogger(ThreatfoxCollector.class);

    private static final String THREATFOX_BASE = "https://threatfox-api.abuse.ch/api/v1/";

    public ThreatfoxCollector(ObjectMapper jsonMapper, String appName, Properties properties) {
        super(jsonMapper, appName, properties, properties.getProperty(CollectorConfig.THREATFOX_TOKEN_CONFIG,
                        CollectorConfig.THREATFOX_TOKEN_DEFAULT), CollectorConfig.THREATFOX_HTTP_TIMEOUT_CONFIG,
                CollectorConfig.THREATFOX_HTTP_TIMEOUT_DEFAULT);
    }

    @Override
    protected org.slf4j.Logger getLogger() {
        return Logger;
    }

    @Override
    protected String getRequestUrl(IPToProcess ip) {
        final var ipInet = InetAddresses.forString(ip.ip());

        // IPv6 is not supported by Threatfox
        if (ipInet instanceof Inet6Address) {
            return null;
        }

        return THREATFOX_BASE;
    }

    @Override
    protected String getRequestUrl(DNToProcess dn) {
        return THREATFOX_BASE;
    }

    @Override
    protected String getAuthTokenHeaderName() {
        return "Auth-Key";
    }

    @Override
    protected String getCollectorName() {
        return NAME;
    }

    @Override
    protected ThreatfoxData mapIPResponseToData(JSONObject jsonResponse) {
        return mapResponseToData(jsonResponse);
    }

    @Override
    protected ThreatfoxData mapDNResponseToData(JSONObject jsonResponse) {
        return mapResponseToData(jsonResponse);
    }

    private ThreatfoxData mapResponseToData(JSONObject jsonResponse) {
        String threatType = null;
        String malware = null;
        Integer confidenceLevel = null;
        List<String> tags = new ArrayList<>();

        var queryStatus = jsonResponse.getString("queryStatus");

        if (queryStatus.equals("ok")) {
            var data = jsonResponse.getJSONArray("data").getJSONObject(0);

            threatType = data.getString("threat_type");
            malware = data.getString("malware");
            confidenceLevel = data.getInt("confidence_level");

            var responseTags = data.getJSONArray("tags");
            for (int i = 0; i < responseTags.length(); i++) {
                tags.add(responseTags.getString(i));
            }
        }

        return new ThreatfoxData(
                threatType,
                malware,
                confidenceLevel,
                tags
        );
    }

    @Override
    protected String getPOSTData(IPToProcess ip) {
        return "{ \"query\": \"search_ioc\", \"search_term\": \"" + ip.ip() + "\", \"exact_match\": true }";
    }

    @Override
    protected String getPOSTData(DNToProcess dn) {
        return "{ \"query\": \"search_ioc\", \"search_term\": \"" + dn.dn() + "\", \"exact_match\": true }";
    }
}
