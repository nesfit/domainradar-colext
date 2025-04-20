package cz.vut.fit.domainradar.standalone.collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.net.InetAddresses;
import cz.vut.fit.domainradar.CollectorConfig;
import cz.vut.fit.domainradar.Common;
import cz.vut.fit.domainradar.models.IPToProcess;
import cz.vut.fit.domainradar.models.repsystems.GreynoiseData;
import cz.vut.fit.domainradar.standalone.BaseIPRepSystemAPICollector;
import org.json.JSONObject;

import java.net.Inet6Address;
import java.util.Properties;

/**
 * A collector that processes IP data using the Greynoise API.
 *
 * @author Matěj Čech
 */
public class GreynoiseCollector extends BaseIPRepSystemAPICollector<GreynoiseData> {
    public static final String NAME = "greynoise";
    public static final String COMPONENT_NAME = "collector-" + NAME;
    private static final org.slf4j.Logger Logger = Common.getComponentLogger(GreynoiseCollector.class);

    private static final String GREYNOISE_BASE = "https://api.greynoise.io/v3/community/";

    public GreynoiseCollector(ObjectMapper jsonMapper, String appName, Properties properties) {
        super(jsonMapper, appName, properties, properties.getProperty(CollectorConfig.GREYNOISE_TOKEN_CONFIG,
                        CollectorConfig.GREYNOISE_TOKEN_DEFAULT), CollectorConfig.GREYNOISE_HTTP_TIMEOUT_CONFIG,
                CollectorConfig.GREYNOISE_HTTP_TIMEOUT_DEFAULT);
    }

    @Override
    protected org.slf4j.Logger getLogger() {
        return Logger;
    }

    @Override
    protected String getRequestUrl(IPToProcess ip) {
        final var ipInet = InetAddresses.forString(ip.ip());

        // IPv6 is not supported by Greynoise
        if (ipInet instanceof Inet6Address) {
            return null;
        }

        return GREYNOISE_BASE;
    }

    @Override
    protected String getAuthTokenHeaderName() {
        return "gn_api_key";
    }

    @Override
    protected String getCollectorName() {
        return NAME;
    }

    @Override
    protected GreynoiseData mapResponseToData(JSONObject jsonResponse) {
        return new GreynoiseData(
                jsonResponse.getBoolean("noise"),
                jsonResponse.getBoolean("riot"),
                jsonResponse.getString("classification")
        );
    }
}