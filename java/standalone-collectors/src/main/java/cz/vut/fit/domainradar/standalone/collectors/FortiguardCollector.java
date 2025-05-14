package cz.vut.fit.domainradar.standalone.collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.vut.fit.domainradar.CollectorConfig;
import cz.vut.fit.domainradar.Common;
import cz.vut.fit.domainradar.models.DNToProcess;
import cz.vut.fit.domainradar.models.IPToProcess;
import cz.vut.fit.domainradar.models.repsystems.FortiguardData;
import cz.vut.fit.domainradar.standalone.BaseCombinedRepSystemAPICollector;
import org.json.JSONObject;

import java.util.Properties;

/**
 * A collector that processes IP and DN data using the Fortiguard API.
 *
 * @author Matěj Čech
 */
public class FortiguardCollector extends BaseCombinedRepSystemAPICollector<FortiguardData> {
    public static final String NAME = "fortiguard";
    public static final String COMPONENT_NAME = "collector-" + NAME;
    private static final org.slf4j.Logger Logger = Common.getComponentLogger(FortiguardCollector.class);

    private static final String FORTIGUARD_BASE = "https://www.fortiguard.com/learnmore/check-blocklist";

    public FortiguardCollector(ObjectMapper jsonMapper, String appName, Properties properties) {
        // Fortiguard does not require an auth token, so a placeholder is used to prevent the collector
        // from being marked as disabled. This token is not sent to Fortiguard, because the method
        // responsible for retrieving the auth token header name returns null.
        super(jsonMapper, appName, properties, "authTokenNotRequired", CollectorConfig.FORTIGUARD_HTTP_TIMEOUT_CONFIG,
                CollectorConfig.FORTIGUARD_HTTP_TIMEOUT_DEFAULT);
    }

    @Override
    protected org.slf4j.Logger getLogger() {
        return Logger;
    }

    @Override
    protected String getRequestUrl(IPToProcess ip) {
        return FORTIGUARD_BASE;
    }

    @Override
    protected String getRequestUrl(DNToProcess dn) {
        return FORTIGUARD_BASE;
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
    protected FortiguardData mapIPResponseToData(JSONObject jsonResponse) {
        return mapResponseToData(jsonResponse);
    }

    @Override
    protected FortiguardData mapDNResponseToData(JSONObject jsonResponse) {
        return mapResponseToData(jsonResponse);
    }

    private FortiguardData mapResponseToData(JSONObject jsonResponse) {
        return new FortiguardData(
                jsonResponse.getBoolean("spam")
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
        return "{" + "\"url\": \"" + target + "\"" + "}";
    }
}
