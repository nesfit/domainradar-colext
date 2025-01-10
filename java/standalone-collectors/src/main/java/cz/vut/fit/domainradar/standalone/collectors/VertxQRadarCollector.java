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
import io.vertx.core.Vertx;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.uritemplate.UriTemplate;
import pl.tlinkowski.unij.api.UniLists;

import org.apache.commons.cli.CommandLine;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.InetAddress;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
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

    public static final String NAME = "qradar";
    public static final String COMPONENT_NAME = "collector-" + NAME;
    private static final org.slf4j.Logger Logger = Common.getComponentLogger(VertxQRadarCollector.class);

    private final Duration _httpTimeout;
    private final int _cacheLifetimeSeconds;
    private final String _baseUrl;
    private final String _token;
    private final int _batchSize;
    private final boolean _disabled;

    public VertxQRadarCollector(@NotNull ObjectMapper jsonMapper, @NotNull String appName,
            @NotNull Properties properties) {
        super(jsonMapper, appName, properties);

        _httpTimeout = Duration.ofMillis(Long.parseLong(
                properties.getProperty(CollectorConfig.QRADAR_TIMEOUT_MS_CONFIG,
                        CollectorConfig.QRADAR_TIMEOUT_MS_DEFAULT)));
        _token = properties.getProperty(CollectorConfig.QRADAR_TOKEN_CONFIG, CollectorConfig.QRADAR_TOKEN_DEFAULT);
        _batchSize = Integer.parseInt(properties.getProperty(CollectorConfig.QRADAR_BATCH_SIZE_CONFIG,
                CollectorConfig.QRADAR_BATCH_SIZE_DEFAULT));
        _cacheLifetimeSeconds = Integer
                .parseInt(properties.getProperty(CollectorConfig.QRADAR_ENTRY_CACHE_LIFETIME_S_CONFIG,
                        CollectorConfig.QRADAR_ENTRY_CACHE_LIFETIME_S_DEFAULT));

        _disabled = _token.isBlank();

        if (_disabled) {
            _baseUrl = "";
        } else {
            var baseUrl = properties.getProperty(CollectorConfig.QRADAR_URL_CONFIG, CollectorConfig.QRADAR_URL_DEFAULT);
            if (!baseUrl.endsWith("/"))
                baseUrl += "/";
            _baseUrl = baseUrl;

            var uri = URI.create(_baseUrl);
            if (!uri.isAbsolute())
                throw new IllegalArgumentException("QRadar URL is not absolute.");
        }
    }

    @Override
    public void run(CommandLine cmd) {
        buildProcessor(_batchSize, _httpTimeout.toMillis());

        // Construct the URIs using placeholders
        final var sourceAddressesByIPEndpoint = UriTemplate.of(
                _baseUrl + "siem/source_addresses?fields=id%2Csource_ip%2Cmagnitude%2Cdomain_id%2Coffense_ids&filter=source_ip%20in%20%28{filter}%29");
        final var offensesEndpoint = UriTemplate.of(
                _baseUrl + "siem/offenses?fields=id%2Cdescription%2Cevent_count%2Cflow_count%2Cdevice_count%2Cseverity%2Cmagnitude%2Clast_updated_time%2Cstatus%2Csource_address_ids&filter=id%20in%20%28{filter}%29");

        // Unfortunately, there is no public API for obtaining ParallelConsumer's Vertx
        // context, so we'll extract their Vertx instance and build our own WebClient
        final Object vertxObj;
        try {
            final var vertxField = VertxParallelEoSStreamProcessor.class
                    .getDeclaredField("vertx");
            vertxField.setAccessible(true);
            vertxObj = vertxField.get(_parallelProcessor);
        } catch (IllegalArgumentException | IllegalAccessException | NoSuchFieldException e) {
            Logger.error("Reflection error: cannot retrieve WebClient from parallel processor", e);
            return;
        }

        final var vertx = (Vertx) vertxObj;
        final var timeoutMs = (int) _httpTimeout.toMillis();
        WebClientOptions webClientOptions = new WebClientOptions()
                .setMaxPoolSize(_maxConcurrency)
                .setHttp2MaxPoolSize(_maxConcurrency)
                .setIdleTimeoutUnit(TimeUnit.MILLISECONDS)
                .setIdleTimeout(timeoutMs)
                .setConnectTimeout(timeoutMs)
                .setReadIdleTimeout(timeoutMs)
                .setSslHandshakeTimeout(timeoutMs)
                .setSslHandshakeTimeoutUnit(TimeUnit.MILLISECONDS)
                .setTrustAll(true); // Debug only

        final var client = WebClient.create(vertx, webClientOptions);

        // A short-lived cache for source addresses
        final var sourceAddressesCache = new ExpiringConcurrentCache<InetAddress, SourceAddressContainer>(
            _cacheLifetimeSeconds, _cacheLifetimeSeconds / 4, TimeUnit.SECONDS);

        // Now we use vertxFuture(...) to process each input batch of requests
        _parallelProcessor.subscribe(UniLists.of(Topics.IN_IP));
        _parallelProcessor.vertxFuture(context -> {
            // Gather IPs from the input batch
            final List<IPToProcess> dnIpPairsToProcess = new ArrayList<>();
            final Set<InetAddress> ipsToProcess = new HashSet<>();

            context.getConsumerRecordsFlattened().forEach(record -> {
                final var ipToProcess = record.key();
                final var ipRequest = record.value();

                // Filter if not intended for this collector
                if (ipRequest != null && ipRequest.collectors() != null
                        && !ipRequest.collectors().contains(NAME)) {
                    return;
                }
                try {
                    var inet = InetAddresses.forString(ipToProcess.ip());
                    ipsToProcess.add(inet);
                    dnIpPairsToProcess.add(ipToProcess);
                } catch (IllegalArgumentException e) {
                    // Invalid address
                    _producer.send(resultRecord(
                            Topics.OUT_IP,
                            ipToProcess,
                            errorResult(ResultCodes.INVALID_ADDRESS, e.getMessage())));
                }
            });

            // If no valid IP to process, return an empty future right away
            if (ipsToProcess.isEmpty()) {
                return Future.succeededFuture(new ConcurrentHashMap<Long, SourceAddressContainer>());
            }

            // Kick off the chain of async calls, returning a Future
            Future<ConcurrentHashMap<Long, SourceAddressContainer>> finalFuture = fetchQRadarDataForIPs(ipsToProcess,
                    sourceAddressesCache, client, sourceAddressesByIPEndpoint, offensesEndpoint);

            // 3) Attach success & failure handlers on the returned future
            finalFuture.onSuccess(sourceAddressesMap -> {
                // Produce final results per IP
                this.produceResultsForIPs(dnIpPairsToProcess, sourceAddressesMap);
            }).onFailure(err -> {
                // If anything in the chain failed, produce error result for each IP
                Logger.error("QRadar data fetch failed: {}", err.getMessage());
                sendAboutAll(_producer, Topics.OUT_QRADAR, dnIpPairsToProcess,
                        errorResult(ResultCodes.INTERNAL_ERROR, err.getMessage()));
            });

            // Return the future so vertxFuture(...) knows when we’re done
            return finalFuture;
        });

        // TODO: Clear the cache after the consumer ends
        sourceAddressesCache.close();
    }

    private void configureQRadarRequest(HttpRequest<?> request) {
        request.putHeader("Accept", "application/json");
        request.putHeader("SEC", _token);
    }

    /**
     * Fetch source addresses + offenses from QRadar, returning a Future that
     * yields a map of "sourceAddressId -> SourceAddressContainer".
     */
    private Future<ConcurrentHashMap<Long, SourceAddressContainer>> fetchQRadarDataForIPs(
            Set<InetAddress> ipsToProcess,
            ExpiringConcurrentCache<InetAddress, SourceAddressContainer> sourceAddressesCache,
            WebClient client,
            UriTemplate sourceAddressesByIPEndpoint,
            UriTemplate offensesEndpoint) {
        // The result map
        final var resultSourceAddresses = new ConcurrentHashMap<Long, SourceAddressContainer>();

        // Build a QRadar query filter that will retrieve IPs which are not in cache yet
        final var filterBuilder = new StringBuilder();
        for (var ip : ipsToProcess) {
            var cached = sourceAddressesCache.get(ip);
            if (cached == null) {
                filterBuilder.append('"')
                        .append(ip.getHostAddress())
                        .append("\",");
            } else {
                // Already in cache
                resultSourceAddresses.put(cached.sourceAddressModel.id, cached);
            }
        }

        // If all IPs were cached, skip directly to success
        if (filterBuilder.length() == 0) {
            return Future.succeededFuture(resultSourceAddresses);
        }

        // Remove trailing comma
        filterBuilder.deleteCharAt(filterBuilder.length() - 1);

        // Request /siem/source_addresses
        var sourceAddrRequest = client.getAbs(sourceAddressesByIPEndpoint);
        sourceAddrRequest.setTemplateParam("filter", filterBuilder.toString());
        this.configureQRadarRequest(sourceAddrRequest);

        final var offensesResponseFuture = sourceAddrRequest
                .send()
                .compose(res -> {
                    var responseModels = res.bodyAsJson(SourceAddressesResponseModel[].class);
                    if (responseModels == null) {
                        return Future.failedFuture("Invalid response from /siem/source_addresses");
                    }
                    if (responseModels.length == 0) {
                        // Means no source address found for these IPs
                        return Future.succeededFuture(null);
                    }

                    // Collect offense IDs
                    Set<Long> offensesToFetch = new HashSet<>();
                    for (var saModel : responseModels) {
                        if (saModel.offenseIds != null) {
                            for (var offId : saModel.offenseIds) {
                                offensesToFetch.add(offId);
                            }
                        }
                        final var container = new SourceAddressContainer(
                                InetAddresses.forString(saModel.sourceIp),
                                saModel,
                                new ArrayList<>());
                        resultSourceAddresses.put(saModel.id(), container);
                        sourceAddressesCache.put(container.address, container);
                    }

                    // If no offenses, skip
                    if (offensesToFetch.isEmpty()) {
                        return Future.succeededFuture(null);
                    }

                    // Request /siem/offenses
                    final var offensesFilter = new StringBuilder();
                    for (var offId : offensesToFetch) {
                        offensesFilter.append(offId).append(',');
                    }
                    offensesFilter.deleteCharAt(offensesFilter.length() - 1);

                    final var offensesReq = client.getAbs(offensesEndpoint);
                    offensesReq.setTemplateParam("filter", offensesFilter.toString());
                    this.configureQRadarRequest(offensesReq);

                    return offensesReq.send();
                });

        // Parse the /siem/offenses response, update and return our result map
        final var resultFuture = offensesResponseFuture
                .map(offensesRes -> {
                    // If previous request returned null, there were no addresses to fetch
                    // Just return the result map
                    if (offensesRes == null) {
                        return resultSourceAddresses;
                    }

                    // Deserialize response
                    final var offenseModels = offensesRes.bodyAsJson(OffenseResponseModel[].class);
                    if (offenseModels == null) {
                        throw new IllegalStateException("Invalid response from /siem/offenses");
                    }

                    // Attach offenses to the corresponding IP's container
                    for (var offModel : offenseModels) {
                        if (offModel.sourceAddressIds == null)
                            continue;

                        final var offense = new QRadarOffense(
                                offModel.id,
                                offModel.description,
                                offModel.eventCount,
                                offModel.flowCount,
                                offModel.deviceCount,
                                offModel.severity,
                                offModel.magnitude,
                                offModel.lastUpdatedTime,
                                offModel.status);
                        for (var addrId : offModel.sourceAddressIds) {
                            final var existingContainer = resultSourceAddresses.computeIfAbsent(addrId, id -> {
                                // Fallback in case an offense references a sourceAddr not in the original input
                                // list
                                return new SourceAddressContainer(
                                        null,
                                        new SourceAddressesResponseModel(id, null, -1, -1, null),
                                        new ArrayList<>());
                            });
                            existingContainer.offenses().add(offense);
                        }
                    }
                    return resultSourceAddresses;
                });

        return resultFuture;
    }

    /**
     * Produces success or error results for each IP based on the final map of
     * SourceAddressContainers.
     */
    private void produceResultsForIPs(
            List<IPToProcess> dnIpPairsToProcess,
            ConcurrentHashMap<Long, SourceAddressContainer> sourceAddresses) {
        for (var ipToProcess : dnIpPairsToProcess) {
            InetAddress ip;
            try {
                ip = InetAddresses.forString(ipToProcess.ip());
            } catch (IllegalArgumentException e) {
                // Already handled as an error earlier
                continue;
            }

            // Find the container whose address == this IP
            SourceAddressContainer container = sourceAddresses.values().stream()
                    .filter(c -> ip.equals(c.address()))
                    .findFirst()
                    .orElse(null);
            var saModel = container == null ? null : container.sourceAddressModel();

            if (saModel == null || saModel.sourceIp() == null) {
                // Not found
                _producer.send(resultRecord(
                        Topics.OUT_QRADAR,
                        ipToProcess,
                        errorResult(ResultCodes.NOT_FOUND, "No source address found in QRadar.")));
            } else {
                // Build final data
                var data = new QRadarData(
                        saModel.id(),
                        saModel.domainId(),
                        saModel.magnitude(),
                        container.offenses());
                _producer.send(resultRecord(Topics.OUT_QRADAR, ipToProcess, successResult(data)));
            }
        }
    }

    protected void buildProcessor(int batchSize, long timeoutMs) {
        _parallelProcessor = VertxParallelStreamProcessor.createEosStreamProcessor(
                this.buildProcessorOptions(batchSize, timeoutMs));
    }

    public @NotNull String getName() {
        return NAME;
    }
}
