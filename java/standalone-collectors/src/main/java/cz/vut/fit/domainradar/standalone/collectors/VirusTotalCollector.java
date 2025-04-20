package cz.vut.fit.domainradar.standalone.collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.vut.fit.domainradar.CollectorConfig;
import cz.vut.fit.domainradar.Common;
import cz.vut.fit.domainradar.models.DNToProcess;
import cz.vut.fit.domainradar.models.IPToProcess;
import cz.vut.fit.domainradar.models.repsystems.VirusTotalData;
import cz.vut.fit.domainradar.standalone.BaseCombinedRepSystemAPICollector;
import org.json.JSONObject;

import java.util.Properties;

/**
 * A collector that processes IP or DN data using the VirusTotal API.
 *
 * @author Matěj Čech
 */
public class VirusTotalCollector extends BaseCombinedRepSystemAPICollector<VirusTotalData> {
    public static final String NAME = "virustotal";
    public static final String COMPONENT_NAME = "collector-" + NAME;
    private static final org.slf4j.Logger Logger = Common.getComponentLogger(VirusTotalCollector.class);

    private static final String VIRUSTOTAL_BASE_IP = "https://www.virustotal.com/api/v3/ip_addresses/";
    private static final String VIRUSTOTAL_BASE_DN = "https://www.virustotal.com/api/v3/domains/";

    public VirusTotalCollector(ObjectMapper jsonMapper, String appName, Properties properties) {
        super(jsonMapper, appName, properties, properties.getProperty(CollectorConfig.VIRUSTOTAL_TOKEN_CONFIG,
                        CollectorConfig.VIRUSTOTAL_TOKEN_DEFAULT), CollectorConfig.VIRUSTOTAL_HTTP_TIMEOUT_CONFIG,
                CollectorConfig.VIRUSTOTAL_HTTP_TIMEOUT_DEFAULT);
    }

    @Override
    protected org.slf4j.Logger getLogger() {
        return Logger;
    }

    @Override
    protected String getRequestUrl(IPToProcess ip) {
        return VIRUSTOTAL_BASE_IP + ip.ip();
    }

    @Override
    protected String getRequestUrl(DNToProcess dn) {
        return VIRUSTOTAL_BASE_DN + dn.dn();
    }

    @Override
    protected String getAuthTokenHeaderName() {
        return "x-apikey";
    }

    @Override
    protected String getCollectorName() {
        return NAME;
    }

    @Override
    protected VirusTotalData mapIPResponseToData(JSONObject jsonResponse) {
        return mapResponseToData(jsonResponse);
    }

    @Override
    protected VirusTotalData mapDNResponseToData(JSONObject jsonResponse) {
        return mapResponseToData(jsonResponse);
    }

    private VirusTotalData mapResponseToData(JSONObject jsonResponse) {
        var attributes = jsonResponse.getJSONObject("data").getJSONObject("attributes");
        var lastStats = attributes.getJSONObject("last_analysis_stats");

        return new VirusTotalData(
                attributes.getInt("reputation"),
                lastStats.getInt("malicious"),
                lastStats.getInt("suspicious"),
                lastStats.getInt("undetected"),
                lastStats.getInt("harmless")
        );
    }
}
