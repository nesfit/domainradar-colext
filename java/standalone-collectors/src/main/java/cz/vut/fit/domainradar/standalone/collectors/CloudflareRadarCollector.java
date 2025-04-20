package cz.vut.fit.domainradar.standalone.collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.net.InetAddresses;
import cz.vut.fit.domainradar.CollectorConfig;
import cz.vut.fit.domainradar.Common;
import cz.vut.fit.domainradar.models.DNToProcess;
import cz.vut.fit.domainradar.models.IPToProcess;
import cz.vut.fit.domainradar.models.repsystems.CloudflareRadarData;
import cz.vut.fit.domainradar.standalone.BaseCombinedRepSystemAPICollector;
import org.json.JSONObject;

import java.net.Inet6Address;
import java.util.Properties;

/**
 * A collector that processes IP and DN data using the Cloudflare Radar API.
 *
 * @author Matěj Čech
 */
public class CloudflareRadarCollector extends BaseCombinedRepSystemAPICollector<CloudflareRadarData> {
    public static final String NAME = "cloudflare-radar";
    public static final String COMPONENT_NAME = "collector-" + NAME;
    private static final org.slf4j.Logger Logger = Common.getComponentLogger(CloudflareRadarCollector.class);

    private static String CLOUDFLARE_BASE =
            "https://api.cloudflare.com/client/v4/accounts/accountid/urlscanner/v2/search?size=1&q=";

    public CloudflareRadarCollector(ObjectMapper jsonMapper, String appName, Properties properties) {
        super(jsonMapper, appName, properties, "Bearer " + properties.getProperty(CollectorConfig.CLOUDFLARERADAR_TOKEN_CONFIG,
                        CollectorConfig.CLOUDFLARERADAR_TOKEN_DEFAULT), CollectorConfig.CLOUDFLARERADAR_HTTP_TIMEOUT_CONFIG,
                CollectorConfig.CLOUDFLARERADAR_HTTP_TIMEOUT_DEFAULT);

        CLOUDFLARE_BASE = CLOUDFLARE_BASE.replace("accountid",
                properties.getProperty(CollectorConfig.CLOUDFLARERADAR_ACCOUNTID_CONFIG));
    }

    @Override
    protected org.slf4j.Logger getLogger() {
        return Logger;
    }

    @Override
    protected String getRequestUrl(IPToProcess ip) {
        final var ipInet = InetAddresses.forString(ip.ip());

        // IPv6 is not supported by Cloudflare Radar
        if (ipInet instanceof Inet6Address) {
            return null;
        }

        return CLOUDFLARE_BASE + "page.ip:" + ip.ip();
    }

    @Override
    protected String getRequestUrl(DNToProcess dn) {
        return CLOUDFLARE_BASE + "page.domain:" + dn.dn();
    }

    @Override
    protected String getAuthTokenHeaderName() {
        return "Authorization";
    }

    @Override
    protected String getCollectorName() {
        return NAME;
    }

    @Override
    protected CloudflareRadarData mapIPResponseToData(JSONObject jsonResponse) {
        return mapResponseToData(jsonResponse);
    }

    @Override
    protected CloudflareRadarData mapDNResponseToData(JSONObject jsonResponse) {
        return mapResponseToData(jsonResponse);
    }

    private CloudflareRadarData mapResponseToData(JSONObject jsonResponse) {
        var results = jsonResponse.getJSONArray("results");
        Boolean malicious = null;

        if (!results.isEmpty()) {
            malicious = results.getJSONObject(0).getJSONObject("verdicts").getBoolean("malicious");
        }

        return new CloudflareRadarData(
                malicious
        );
    }
}
