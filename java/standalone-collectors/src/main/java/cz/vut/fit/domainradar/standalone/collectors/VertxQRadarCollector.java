package cz.vut.fit.domainradar.standalone.collectors;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.net.InetAddresses;
import cz.vut.fit.domainradar.CollectorConfig;
import cz.vut.fit.domainradar.Common;
import cz.vut.fit.domainradar.ExpiringConcurrentCache;
import cz.vut.fit.domainradar.Topics;
import cz.vut.fit.domainradar.models.IPToProcess;
import cz.vut.fit.domainradar.models.ResultCodes;
import cz.vut.fit.domainradar.models.ip.QRadarData;
import cz.vut.fit.domainradar.models.requests.IPRequest;
import cz.vut.fit.domainradar.standalone.IPStandaloneCollector;
import io.confluent.parallelconsumer.vertx.VertxParallelStreamProcessor;
import io.vertx.core.Future;
import io.vertx.uritemplate.UriTemplate;
import org.apache.commons.cli.CommandLine;
import org.checkerframework.checker.units.qual.A;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.InetAddress;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class VertxQRadarCollector
        extends IPStandaloneCollector<QRadarData, VertxParallelStreamProcessor<IPToProcess, IPRequest>> {

    public record SourceAddressesResponseModel(
            @JsonProperty("id") long id,
            @JsonProperty("source_ip") @NotNull String sourceIp,
            @JsonProperty("domain_id") long domainId,
            @JsonProperty("magnitude") long magnitude,
            @JsonProperty("offense_ids") long[] offenseIds) {
    }

    public record OffenseResponseModel(
            @JsonProperty("id") int id,
            @JsonProperty("description") @Nullable String description,
            @JsonProperty("event_count") int eventCount,
            @JsonProperty("flow_count") int flowCount,
            @JsonProperty("device_count") int deviceCount,
            @JsonProperty("severity") double severity,
            @JsonProperty("magnitude") double magnitude,
            @JsonProperty("last_updated_time") long lastUpdatedTime,
            @JsonProperty("status") @Nullable String status,
            @JsonProperty("source_address_ids") int[] sourceAddressIds) {
    }

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

        // Stores the source address IDs for already seen IPs
        final var sourceAddressToIdCache = new ExpiringConcurrentCache<InetAddress, QRadarData>(60, 10, TimeUnit.SECONDS);

        final var sourceAddressesByIPEndpoint = UriTemplate.of(_baseUrl
                + "siem/source_addresses?fields=id%2Csource_ip%2Cmagnitude%2Cdomain_id%2Coffense_ids&filter=source_ip%20in%20%28{filter}%29");
        // final var sourceAddressesByIDEndpoint = UriTemplate.of(_baseUrl
        // + "siem/source_addresses?fields=magnitude%2Cdomain_id%2Coffense_ids&filter=id%20in%20%28{filter}%29");

        final var offensesEndpoint = UriTemplate.of(_baseUrl
                + "siem/offenses?fields=id%2Cdescription%2Cevent_count%2Cflow_count%2Cdevice_count%2Cseverity%2Cmagnitude%2Clast_updated_time%2Cstatus%2Csource_address_ids&filter=id%20in%20%28{filter}%29");

        _parallelProcessor.vertxHttpWebClient((client, context) -> {
            var ipsToProcess = new HashSet<InetAddress>();
            for (var entry : context.getConsumerRecordsFlattened()) {
                final var inputIpToProcess = entry.key();
                final var request = entry.value();
                if (request != null && request.collectors() != null && !request.collectors().contains(NAME))
                    continue;

                try {
                    final var ip = InetAddresses.forString(inputIpToProcess.ip());
                    ipsToProcess.add(ip);
                } catch (IllegalArgumentException e) {
                    _producer.send(resultRecord(Topics.OUT_IP, inputIpToProcess,
                            errorResult(ResultCodes.INVALID_ADDRESS, e.getMessage())));
                }
            }

            if (ipsToProcess.isEmpty())
                return Future.succeededFuture();

            final var sourceAddressObjects = new ConcurrentHashMap<InetAddress, QRadarData>();

            // full IPv4 incl. "", is 18 chars long string
            final var sourceAddressesToFetchFilter = new StringBuilder(ipsToProcess.size() * 18);

            for (var ip : ipsToProcess) {
                var cachedEntry = sourceAddressToIdCache.get(ip);
                if (cachedEntry != null) {
                    sourceAddressObjects.put(ip, cachedEntry);
                } else {
                    sourceAddressesToFetchFilter.append('"');
                    sourceAddressesToFetchFilter.append(ip.getHostAddress());
                    sourceAddressesToFetchFilter.append("\",");
                }
            }
            if (sourceAddressesToFetchFilter.isEmpty()) {
                // TODO: return
            }
            sourceAddressesToFetchFilter.deleteCharAt(sourceAddressesToFetchFilter.length() - 1);

            var sourceAddressesRequest = client.get(sourceAddressesByIPEndpoint);
            sourceAddressesRequest.setTemplateParam("filter", sourceAddressesToFetchFilter.toString());

            return sourceAddressesRequest.send().compose(res -> {
                var responseModels = res.bodyAsJson(SourceAddressesResponseModel[].class);
                if (responseModels == null || responseModels.length == 0) {
                    return Future.failedFuture("Invalid response from /siem/source_addresses.");
                }

                var offensesToFetch = new HashSet<Long>();
                for (var sourceAddressModel : responseModels) {
                    if (sourceAddressModel.offenseIds != null) {
                        for (var offenseId : sourceAddressModel.offenseIds) {
                            offensesToFetch.add(offenseId);
                        }

                        sourceAddressObjects.put(InetAddresses.forString(sourceAddressModel.sourceIp),
                                new QRadarData(sourceAddressModel.id, sourceAddressModel.domainId,
                                        sourceAddressModel.magnitude, new ArrayList<>()));
                    }
                }

                if (offensesToFetch.isEmpty()) {
                    return Future.succeededFuture();
                }

                final var offensesToResolveFilter = new StringBuilder(ipsToProcess.size() * 18);
                for (var offenseToFetchId : offensesToFetch) {
                    offensesToResolveFilter.append(offenseToFetchId);
                    offensesToResolveFilter.append(',');
                }

                offensesToResolveFilter.deleteCharAt(offensesToResolveFilter.length() - 1);

                var offensesRequest = client.get(sourceAddressesByIPEndpoint);
                offensesRequest.setTemplateParam("filter", offensesToResolveFilter.toString());
                return offensesRequest.send();
            }).compose(res -> {

            });
        }, res -> {
        });

        sourceAddressToIdCache.close();
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
