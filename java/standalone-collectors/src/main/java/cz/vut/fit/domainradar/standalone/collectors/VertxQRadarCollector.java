package cz.vut.fit.domainradar.standalone.collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.vut.fit.domainradar.CollectorConfig;
import cz.vut.fit.domainradar.Common;
import cz.vut.fit.domainradar.models.IPToProcess;
import cz.vut.fit.domainradar.models.ip.QRadarData;
import cz.vut.fit.domainradar.models.requests.IPRequest;
import cz.vut.fit.domainradar.standalone.IPStandaloneCollector;
import io.confluent.parallelconsumer.vertx.VertxParallelStreamProcessor;
import io.vertx.uritemplate.UriTemplate;
import org.apache.commons.cli.CommandLine;
import org.jetbrains.annotations.NotNull;

import java.net.URI;
import java.time.Duration;
import java.util.Properties;

public class VertxQRadarCollector
        extends IPStandaloneCollector<QRadarData, VertxParallelStreamProcessor<IPToProcess, IPRequest>> {
    public static final String NAME = "nerd";
    public static final String COMPONENT_NAME = "collector-" + NAME;
    private static final org.slf4j.Logger Logger = Common.getComponentLogger(NERDCollector.class);

    private final Duration _httpTimeout;
    private final String _baseUrl;
    private final String _token;
    private final int _batchSize;


    public VertxQRadarCollector(@NotNull ObjectMapper jsonMapper, @NotNull String appName, @NotNull Properties properties) {
        super(jsonMapper, appName, properties);

        _httpTimeout = Duration.ofMillis(Long.parseLong(
                properties.getProperty(CollectorConfig.QRADAR_TIMEOUT_MS_CONFIG,
                        CollectorConfig.QRADAR_TIMEOUT_MS_DEFAULT)));
        _token = properties.getProperty(CollectorConfig.QRADAR_TOKEN_CONFIG, CollectorConfig.QRADAR_TOKEN_DEFAULT);
        _batchSize = Integer.parseInt(properties.getProperty(CollectorConfig.QRADAR_BATCH_SIZE_CONFIG,
                CollectorConfig.QRADAR_BATCH_SIZE_DEFAULT));
        if (_token.isBlank())
            throw new IllegalArgumentException("QRadar token is not set.");

        var baseUrl = properties.getProperty(CollectorConfig.QRADAR_URL_CONFIG, CollectorConfig.QRADAR_URL_DEFAULT);
        if (!baseUrl.endsWith("/"))
            baseUrl += "/";
        _baseUrl = baseUrl;

        var uri = URI.create(_baseUrl);
        if (!uri.isAbsolute())
            throw new IllegalArgumentException("QRadar URL is not absolute.");
    }

    @Override
    public void run(CommandLine cmd) {
        this.buildProcessor(_batchSize, _httpTimeout.toMillis());

        final var sourceAddressesByIPEndpoint = UriTemplate.of(_baseUrl + "siem/source_addresses?fields=magnitude%2Cdomain_id%2Coffense_ids&filter=source_ip%20in%20%28{filter}%29");
        final var sourceAddressesByIDEndpoint = UriTemplate.of(_baseUrl + "siem/source_addresses?fields=magnitude%2Cdomain_id%2Coffense_ids&filter=id%20in%20%28{filter}%29");

        final var offensesEndpoint = UriTemplate.of(_baseUrl + "siem/offenses?");
                _parallelProcessor.vertxHttpWebClient((client, context) -> {

                }, r -> {
                });
    }

    protected void buildProcessor(int batchSize, long timeoutMs) {
        _parallelProcessor = VertxParallelStreamProcessor.createEosStreamProcessor(
                this.buildProcessorOptions(batchSize, timeoutMs)
        );

    }

    public @NotNull String getName() {
        return NAME;
    }
}
