package cz.vut.fit.domainradar.standalone.collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.vut.fit.domainradar.CollectorConfig;
import cz.vut.fit.domainradar.Common;
import cz.vut.fit.domainradar.models.DNToProcess;
import cz.vut.fit.domainradar.models.IPToProcess;
import cz.vut.fit.domainradar.models.repsystems.HybridAnalysisData;
import cz.vut.fit.domainradar.standalone.BaseCombinedRepSystemAPICollector;
import org.json.JSONObject;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * A collector that processes IP and DN data using the HybridAnalysis API.
 *
 * @author Matěj Čech
 */
public class HybridAnalysisCollector extends BaseCombinedRepSystemAPICollector<HybridAnalysisData> {
    public static final String NAME = "hybrid-analysis";
    public static final String COMPONENT_NAME = "collector-" + NAME;
    private static final org.slf4j.Logger Logger = Common.getComponentLogger(HybridAnalysisCollector.class);

    private static final String HYBRIDANALYSIS_BASE = "https://www.hybrid-analysis.com/api/v2/search/terms";

    public HybridAnalysisCollector(ObjectMapper jsonMapper, String appName, Properties properties) {
        super(jsonMapper, appName, properties, properties.getProperty(CollectorConfig.HYBRIDANALYSIS_TOKEN_CONFIG,
                        CollectorConfig.HYBRIDANALYSIS_TOKEN_DEFAULT), CollectorConfig.HYBRIDANALYSIS_HTTP_TIMEOUT_CONFIG,
                CollectorConfig.HYBRIDANALYSIS_HTTP_TIMEOUT_DEFAULT);
    }

    @Override
    protected org.slf4j.Logger getLogger() {
        return Logger;
    }

    @Override
    protected String getRequestUrl(IPToProcess ip) {
        return HYBRIDANALYSIS_BASE;
    }

    @Override
    protected String getRequestUrl(DNToProcess dn) {
        return HYBRIDANALYSIS_BASE;
    }

    @Override
    protected String getAuthTokenHeaderName() {
        return "api-key";
    }

    @Override
    protected String getCollectorName() {
        return NAME;
    }

    @Override
    protected HybridAnalysisData mapIPResponseToData(JSONObject jsonResponse) {
        return mapResponseToData(jsonResponse);
    }

    @Override
    protected HybridAnalysisData mapDNResponseToData(JSONObject jsonResponse) {
        return mapResponseToData(jsonResponse);
    }

    private HybridAnalysisData mapResponseToData(JSONObject jsonResponse) {
        var data = jsonResponse.getJSONArray("result");
        var dataLen = data.length();

        int maliciousCnt = 0;
        int suspiciousCnt = 0;
        int noThreatCnt = 0;
        int whitelistedCnt = 0;

        int worstScore = dataLen == 0 ? -1 : 0;
        int bestScore = dataLen == 0 ? -1 : 100;
        int sumScore = 0;
        int nullScoreCnt = 0;

        for (int i = 0; i < dataLen; i++) {
            JSONObject singleResult = data.getJSONObject(i);
            String verdict = singleResult.getString("verdict");

            switch (verdict.toLowerCase()) {
                case "malicious":
                    maliciousCnt++;
                    break;
                case "suspicious":
                    suspiciousCnt++;
                    break;
                case "whitelisted":
                    whitelistedCnt++;
                    break;
                case "no specific threat":
                    noThreatCnt++;
                    break;
            }

            if (singleResult.isNull("threat_score")) {
                nullScoreCnt++;
            }
            else {
                int curr_score = singleResult.getInt("threat_score");
                sumScore += curr_score;

                if (curr_score > worstScore) {
                    worstScore = curr_score;
                }

                if (curr_score < bestScore) {
                    bestScore = curr_score;
                }
            }
        }

        return new HybridAnalysisData(
                maliciousCnt,
                suspiciousCnt,
                noThreatCnt,
                whitelistedCnt,
                dataLen == nullScoreCnt ? -1 : worstScore,
                dataLen == nullScoreCnt ? -1 : bestScore,
                dataLen == 0 ? -1 : sumScore / (dataLen - nullScoreCnt),
                nullScoreCnt
        );
    }

    @Override
    protected String getUrlEncodedData(IPToProcess ip) {
        return "host=" + URLEncoder.encode(ip.ip(), StandardCharsets.UTF_8);
    }

    @Override
    protected String getUrlEncodedData(DNToProcess dn) {
        return "domain=" + URLEncoder.encode(dn.dn(), StandardCharsets.UTF_8);
    }
}