package cz.vut.fit.domainradar.standalone.collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.vut.fit.domainradar.CollectorConfig;
import cz.vut.fit.domainradar.Common;
import cz.vut.fit.domainradar.models.DNToProcess;
import cz.vut.fit.domainradar.models.IPToProcess;
import cz.vut.fit.domainradar.models.repsystems.PulsediveData;
import cz.vut.fit.domainradar.standalone.BaseCombinedRepSystemAPICollector;
import org.json.JSONObject;

import java.util.Properties;

/**
 * A collector that processes IP and DN data using the Pulsedive API.
 *
 * @author Matěj Čech
 */
public class PulsediveCollector extends BaseCombinedRepSystemAPICollector<PulsediveData> {
    public static final String NAME = "pulsedive";
    public static final String COMPONENT_NAME = "collector-" + NAME;
    private static final org.slf4j.Logger Logger = Common.getComponentLogger(PulsediveCollector.class);

    private static String PULSEDIVE_BASE = "https://pulsedive.com/api/info.php";

    public PulsediveCollector(ObjectMapper jsonMapper, String appName, Properties properties) {
        super(jsonMapper, appName, properties, properties.getProperty(CollectorConfig.PULSEDIVE_TOKEN_CONFIG,
                        CollectorConfig.PULSEDIVE_TOKEN_DEFAULT), CollectorConfig.PULSEDIVE_HTTP_TIMEOUT_CONFIG,
                CollectorConfig.PULSEDIVE_HTTP_TIMEOUT_DEFAULT);

        PULSEDIVE_BASE += "?key=" + properties.getProperty(CollectorConfig.PULSEDIVE_TOKEN_CONFIG,
                CollectorConfig.PULSEDIVE_TOKEN_DEFAULT);
    }

    @Override
    protected org.slf4j.Logger getLogger() {
        return Logger;
    }

    @Override
    protected String getRequestUrl(IPToProcess ip) {
        return PULSEDIVE_BASE + "&indicator=" + ip.ip() + "&pretty=1";
    }

    @Override
    protected String getRequestUrl(DNToProcess dn) {
        return PULSEDIVE_BASE + "&indicator=" + dn.dn() + "&pretty=1";
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
    protected PulsediveData mapIPResponseToData(JSONObject jsonResponse) {
        return mapResponseToData(jsonResponse);
    }

    @Override
    protected PulsediveData mapDNResponseToData(JSONObject jsonResponse) {
        return mapResponseToData(jsonResponse);
    }

    private PulsediveData mapResponseToData(JSONObject jsonResponse) {
        return new PulsediveData(
                jsonResponse.getString("risk"),
                jsonResponse.getString("risk_recommended"),
                jsonResponse.getInt("manualrisk")
        );
    }
}