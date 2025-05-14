package cz.vut.fit.domainradar.standalone.collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.net.InetAddresses;
import cz.vut.fit.domainradar.CollectorConfig;
import cz.vut.fit.domainradar.Common;
import cz.vut.fit.domainradar.models.IPToProcess;
import cz.vut.fit.domainradar.models.repsystems.CriminalIPData;
import cz.vut.fit.domainradar.standalone.BaseIPRepSystemAPICollector;
import org.json.JSONObject;

import java.net.Inet6Address;
import java.util.Properties;

/**
 * A collector that processes IP data using the CriminalIP API.
 *
 * @author Matěj Čech
 */
public class CriminalIPCollector extends BaseIPRepSystemAPICollector<CriminalIPData> {
    public static final String NAME = "criminalip";
    public static final String COMPONENT_NAME = "collector-" + NAME;
    private static final org.slf4j.Logger Logger = Common.getComponentLogger(CriminalIPCollector.class);

    private static final String CRIMINALIP_BASE = "https://api.criminalip.io/v1/asset/ip/report";

    public CriminalIPCollector(ObjectMapper jsonMapper, String appName, Properties properties) {
        super(jsonMapper, appName, properties, properties.getProperty(CollectorConfig.CRIMINALIP_TOKEN_CONFIG,
                        CollectorConfig.CRIMINALIP_TOKEN_DEFAULT), CollectorConfig.CRIMINALIP_HTTP_TIMEOUT_CONFIG,
                CollectorConfig.CRIMINALIP_HTTP_TIMEOUT_DEFAULT);
    }

    @Override
    protected org.slf4j.Logger getLogger() {
        return Logger;
    }

    @Override
    protected String getRequestUrl(IPToProcess ip) {
        final var ipInet = InetAddresses.forString(ip.ip());

        // IPv6 is not supported by CriminalIP
        if (ipInet instanceof Inet6Address) {
            return null;
        }

        return CRIMINALIP_BASE + "?ip=" + ip.ip() + "&full=true";
    }

    @Override
    protected String getAuthTokenHeaderName() {
        return "x-api-key";
    }

    @Override
    protected String getCollectorName() {
        return NAME;
    }

    @Override
    protected CriminalIPData mapResponseToData(JSONObject jsonResponse) {
        var score = jsonResponse.getJSONObject("score");

        return new CriminalIPData(
                score.getString("inbound"),
                score.getString("outbound")
        );
    }
}
