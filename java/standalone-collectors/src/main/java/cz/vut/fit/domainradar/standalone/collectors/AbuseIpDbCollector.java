package cz.vut.fit.domainradar.standalone.collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.vut.fit.domainradar.CollectorConfig;
import cz.vut.fit.domainradar.Common;
import cz.vut.fit.domainradar.models.IPToProcess;
import cz.vut.fit.domainradar.models.repsystems.AbuseIpDbData;
import cz.vut.fit.domainradar.standalone.BaseIPRepSystemAPICollector;
import org.json.JSONObject;

import java.util.Properties;

/**
 * A collector that processes IP data using the AbuseIPDB API.
 *
 * @author Matěj Čech
 */
public class AbuseIpDbCollector extends BaseIPRepSystemAPICollector<AbuseIpDbData> {
    public static final String NAME = "abuseipdb";
    public static final String COMPONENT_NAME = "collector-" + NAME;
    private static final org.slf4j.Logger Logger = Common.getComponentLogger(AbuseIpDbCollector.class);

    private static final String ABUSEIPDB_BASE = "https://api.abuseipdb.com/api/v2/check";

    public AbuseIpDbCollector(ObjectMapper jsonMapper, String appName, Properties properties) {
        super(jsonMapper, appName, properties, properties.getProperty(CollectorConfig.ABUSEIPDB_TOKEN_CONFIG,
                        CollectorConfig.ABUSEIPDB_TOKEN_DEFAULT), CollectorConfig.ABUSEIPDB_HTTP_TIMEOUT_CONFIG,
                CollectorConfig.ABUSEIPDB_HTTP_TIMEOUT_DEFAULT);
    }

    @Override
    protected org.slf4j.Logger getLogger() {
        return Logger;
    }

    @Override
    protected String getRequestUrl(IPToProcess ip) {
        return ABUSEIPDB_BASE + "?ipAddress=" + ip.ip();
    }

    @Override
    protected String getAuthTokenHeaderName() {
        return "Key";
    }

    @Override
    protected String getCollectorName() {
        return NAME;
    }

    @Override
    protected AbuseIpDbData mapResponseToData(JSONObject jsonResponse) {
        var data = jsonResponse.getJSONObject("data");

        return new AbuseIpDbData(
                data.getInt("abuseConfidenceScore"),
                data.isNull("isWhitelisted") ? null : data.getBoolean("isWhitelisted"),
                data.isNull("isTor") ? null : data.getBoolean("isTor"),
                data.getInt("totalReports")
        );
    }
}