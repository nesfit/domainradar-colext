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
import cz.vut.fit.domainradar.models.ip.QRadarData.QRadarOffense;
import cz.vut.fit.domainradar.models.requests.IPRequest;
import cz.vut.fit.domainradar.standalone.IPStandaloneCollector;
import io.confluent.parallelconsumer.vertx.VertxParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.vertx.VertxParallelStreamProcessor;
import io.vertx.core.Future;
import io.vertx.ext.web.client.WebClient;
import io.vertx.uritemplate.UriTemplate;
import org.apache.commons.cli.CommandLine;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.InetAddress;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
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

    private record SourceAddressContainer(
            InetAddress address,
            SourceAddressesResponseModel sourceAddressModel,
            List<QRadarOffense> offenses) {
    };

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
            @JsonProperty("source_address_ids") long[] sourceAddressIds) {
    }

    public static final String NAME = "nerd";
    public static final String COMPONENT_NAME = "collector-" + NAME;
    private static final org.slf4j.Logger Logger = Common.getComponentLogger(NERDCollector.class);

    private final Duration _httpTimeout;
    private final String _baseUrl;
    private final String _token;
    private final int _batchSize;

    public VertxQRadarCollector(@NotNull ObjectMapper jsonMapper, @NotNull String appName,
            @NotNull Properties properties) {
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

        final var sourceAddressesByIPEndpoint = UriTemplate.of(_baseUrl
                + "siem/source_addresses?fields=id%2Csource_ip%2Cmagnitude%2Cdomain_id%2Coffense_ids&filter=source_ip%20in%20%28{filter}%29");
        // final var sourceAddressesByIDEndpoint = UriTemplate.of(_baseUrl
        // +
        // "siem/source_addresses?fields=magnitude%2Cdomain_id%2Coffense_ids&filter=id%20in%20%28{filter}%29");

        final var offensesEndpoint = UriTemplate.of(_baseUrl
                + "siem/offenses?fields=id%2Cdescription%2Cevent_count%2Cflow_count%2Cdevice_count%2Cseverity%2Cmagnitude%2Clast_updated_time%2Cstatus%2Csource_address_ids&filter=id%20in%20%28{filter}%29");

        Object clientObj;
        try {
            clientObj = VertxParallelEoSStreamProcessor.class.getDeclaredField("webClient").get(_parallelProcessor);
        } catch (IllegalArgumentException | IllegalAccessException | NoSuchFieldException e) {
            Logger.error("Reflection error", e);
            return;
        }

        // Stores the source address IDs for already seen IPs
        final var sourceAddressesCache = new ExpiringConcurrentCache<InetAddress, SourceAddressContainer>(60, 10,
            TimeUnit.SECONDS);

        final var client = (WebClient) clientObj;

        _parallelProcessor.vertxFuture((context) -> {
            var ipsToProcess = new HashSet<InetAddress>();
            var dnIpPairsToProcess = new ArrayList<IPToProcess>();

            for (var entry : context.getConsumerRecordsFlattened()) {
                final var inputIpToProcess = entry.key();
                final var request = entry.value();
                if (request != null && request.collectors() != null && !request.collectors().contains(NAME))
                    continue;

                try {
                    final var ip = InetAddresses.forString(inputIpToProcess.ip());
                    ipsToProcess.add(ip);
                    dnIpPairsToProcess.add(inputIpToProcess);
                } catch (IllegalArgumentException e) {
                    _producer.send(resultRecord(Topics.OUT_IP, inputIpToProcess,
                            errorResult(ResultCodes.INVALID_ADDRESS, e.getMessage())));
                }
            }

            if (ipsToProcess.isEmpty())
                return Future.succeededFuture();

            final var sourceAddresses = new ConcurrentHashMap<Long, SourceAddressContainer>();

            // full IPv4 incl. "", is 18 chars long string
            final var sourceAddressesToFetchFilter = new StringBuilder(ipsToProcess.size() * 18);

            for (var ip : ipsToProcess) {
                var cachedEntry = sourceAddressesCache.get(ip);
                if (cachedEntry != null) {
                    sourceAddresses.put(cachedEntry.sourceAddressModel.id, cachedEntry);
                } else {
                    sourceAddressesToFetchFilter.append('"');
                    sourceAddressesToFetchFilter.append(ip.getHostAddress());
                    sourceAddressesToFetchFilter.append("\",");
                }
            }

            if (sourceAddressesToFetchFilter.isEmpty()) {
                return Future.succeededFuture(sourceAddresses);
            }

            sourceAddressesToFetchFilter.deleteCharAt(sourceAddressesToFetchFilter.length() - 1);

            var sourceAddressesRequest = client.get(sourceAddressesByIPEndpoint);
            sourceAddressesRequest.setTemplateParam("filter", sourceAddressesToFetchFilter.toString());

            return sourceAddressesRequest.send()
                    .compose(res -> {
                        var responseModels = res.bodyAsJson(SourceAddressesResponseModel[].class);
                        if (responseModels == null) {
                            return Future.failedFuture("Invalid response from /siem/source_addresses.");
                        }

                        if (responseModels.length == 0) {
                            Logger.debug("No source address found.");
                            return Future.succeededFuture();
                        }

                        var offensesToFetch = new HashSet<Long>();
                        for (var sourceAddressModel : responseModels) {
                            if (sourceAddressModel.offenseIds != null) {
                                for (var offenseId : sourceAddressModel.offenseIds) {
                                    offensesToFetch.add(offenseId);
                                }

                                sourceAddresses.put(sourceAddressModel.id,
                                        new SourceAddressContainer(InetAddresses.forString(sourceAddressModel.sourceIp),
                                                sourceAddressModel, new ArrayList<>()));
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

                        var offensesRequest = client.get(offensesEndpoint);
                        offensesRequest.setTemplateParam("filter", offensesToResolveFilter.toString());
                        return offensesRequest.send();
                    }).compose(res -> {
                        if (res == null) {
                            // Propagate null
                            return Future.succeededFuture();
                        }

                        var responseModels = res.bodyAsJson(OffenseResponseModel[].class);
                        if (responseModels == null) {
                            return Future.failedFuture("Invalid response from /siem/offenses.");
                        }

                        if (responseModels.length == 0) {
                            Logger.debug("No offense objects found.");
                            return Future.succeededFuture();
                        }

                        for (var offenseModel : responseModels) {
                            if (offenseModel.sourceAddressIds == null) {
                                continue;
                            }

                            var offense = new QRadarOffense(offenseModel.id, offenseModel.description,
                                    offenseModel.eventCount, offenseModel.flowCount, offenseModel.deviceCount,
                                    offenseModel.severity,
                                    offenseModel.magnitude, offenseModel.lastUpdatedTime, offenseModel.status);

                            for (var offensesSourceAddressId : offenseModel.sourceAddressIds) {
                                var sourceAddressContainer = sourceAddresses.computeIfAbsent(offensesSourceAddressId,
                                        id -> new SourceAddressContainer(null,
                                                new SourceAddressesResponseModel(id, null, -1, -1, null),
                                                new ArrayList<>()));

                                sourceAddressContainer.offenses.add(offense);
                            }
                        }

                        return Future.succeededFuture(sourceAddresses);
                    }).compose(res -> {
                        if (res == null) {
                            sendAboutAll(_producer, Topics.OUT_QRADAR, dnIpPairsToProcess, errorResult(ResultCodes.NOT_FOUND, null));
                            return Future.succeededFuture();
                        }

                        // TODO: Optimise by keeping reference to the source dnip
                        

                        return Future.succeededFuture();
                    });
        });

        sourceAddressesCache.close();
    }

    protected void buildProcessor(int batchSize, long timeoutMs) {
        _parallelProcessor = VertxParallelStreamProcessor.createEosStreamProcessor(
                this.buildProcessorOptions(batchSize, timeoutMs));

    }

    public @NotNull String getName() {
        return NAME;
    }
}
