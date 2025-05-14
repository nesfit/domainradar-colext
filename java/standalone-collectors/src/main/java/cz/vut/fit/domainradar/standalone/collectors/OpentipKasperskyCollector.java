package cz.vut.fit.domainradar.standalone.collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.net.InetAddresses;
import cz.vut.fit.domainradar.CollectorConfig;
import cz.vut.fit.domainradar.Common;
import cz.vut.fit.domainradar.models.DNToProcess;
import cz.vut.fit.domainradar.models.IPToProcess;
import cz.vut.fit.domainradar.models.repsystems.OpentipKasperskyData;
import cz.vut.fit.domainradar.standalone.BaseCombinedRepSystemAPICollector;
import org.json.JSONArray;
import org.json.JSONObject;

import java.net.Inet6Address;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * A collector that processes IP and DN data using the Opentip Kaspersky API.
 *
 * @author Matěj Čech
 */
public class OpentipKasperskyCollector extends BaseCombinedRepSystemAPICollector<OpentipKasperskyData> {
    public static final String NAME = "opentip-kaspersky";
    public static final String COMPONENT_NAME = "collector" + NAME;
    private static final org.slf4j.Logger Logger = Common.getComponentLogger(OpentipKasperskyCollector.class);

    private static final String OPENTIP_KASPERSKY_BASE = "https://opentip.kaspersky.com/api/v1/search/";
    private static final String OPENTIP_KASPERSKY_BASE_IP = OPENTIP_KASPERSKY_BASE + "ip";
    private static final String OPENTIP_KASPERSKY_BASE_DN = OPENTIP_KASPERSKY_BASE + "domain";

    public OpentipKasperskyCollector(ObjectMapper jsonMapper, String appName, Properties properties) {
        super(jsonMapper, appName, properties, properties.getProperty(CollectorConfig.OPENTIPKASPERSKY_TOKEN_CONFIG,
                        CollectorConfig.OPENTIPKASPERSKY_TOKEN_DEFAULT), CollectorConfig.OPENTIPKASPERSKY_HTTP_TIMEOUT_CONFIG,
                CollectorConfig.OPENTIPKASPERSKY_HTTP_TIMEOUT_DEFAULT);
    }

    @Override
    protected org.slf4j.Logger getLogger() {
        return Logger;
    }

    @Override
    protected String getRequestUrl(IPToProcess ip) {
        final var ipInet = InetAddresses.forString(ip.ip());

        // IPv6 is not supported by Opentip Kaspersky
        if (ipInet instanceof Inet6Address) {
            return null;
        }

        return OPENTIP_KASPERSKY_BASE_IP + "?request=" + ip.ip();
    }

    @Override
    protected String getRequestUrl(DNToProcess dn) {
        return OPENTIP_KASPERSKY_BASE_DN + "?request=" + dn.dn();
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
    protected OpentipKasperskyData mapIPResponseToData(JSONObject jsonResponse) {
        return mapResponseToData(jsonResponse, "IpGeneralInfo");
    }

    @Override
    protected OpentipKasperskyData mapDNResponseToData(JSONObject jsonResponse) {
        return mapResponseToData(jsonResponse, "DomainGeneralInfo");
    }

    private OpentipKasperskyData mapResponseToData(JSONObject jsonResponse, String generalInfoElementName) {
        List<String> categories = new ArrayList<>();
        List<String> categoryZones = new ArrayList<>();

        var generalInfo = jsonResponse.getJSONObject(generalInfoElementName);

        if (generalInfo != null && generalInfo.has("CategoriesWithZone")) {
            JSONArray categoriesWithZone = generalInfo.getJSONArray("CategoriesWithZone");

            for (int i = 0; i < categoriesWithZone.length(); i++) {
                JSONObject category = categoriesWithZone.getJSONObject(i);
                categories.add(category.getString("Name"));
                categoryZones.add(category.getString("Zone"));
            }
        }

        return new OpentipKasperskyData(
                jsonResponse.getString("Zone"),
                categories,
                categoryZones
        );
    }
}
