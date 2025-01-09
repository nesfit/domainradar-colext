package cz.vut.fit.domainradar.standalone.collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.net.InetAddresses;
import cz.vut.fit.domainradar.CollectorConfig;
import cz.vut.fit.domainradar.Common;
import cz.vut.fit.domainradar.Topics;
import cz.vut.fit.domainradar.models.IPToProcess;
import cz.vut.fit.domainradar.models.ResultCodes;
import cz.vut.fit.domainradar.models.ip.NERDData;
import cz.vut.fit.domainradar.models.requests.IPRequest;
import cz.vut.fit.domainradar.models.results.CommonIPResult;
import cz.vut.fit.domainradar.standalone.IPStandaloneCollector;
import io.confluent.parallelconsumer.ParallelStreamProcessor;
import org.apache.commons.cli.CommandLine;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.jetbrains.annotations.NotNull;
import pl.tlinkowski.unij.api.UniLists;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpConnectTimeoutException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A collector that processes IP data using the NERD API.
 *
 * @author Ondřej Ondryáš
 */
public class NERDCollector extends IPStandaloneCollector<NERDData, ParallelStreamProcessor<IPToProcess, IPRequest>> {
    public static final String NAME = "nerd";
    public static final String COMPONENT_NAME = "collector-" + NAME;
    private static final org.slf4j.Logger Logger = Common.getComponentLogger(NERDCollector.class);

    private static final String NERD_BASE = "https://nerd.cesnet.cz/nerd/api/v1/";

    private final ExecutorService _executor;
    private final String _token;
    private HttpClient _client;

    private final Duration _httpTimeout;
    private final int _batchSize;

    private final boolean _disabled;

    public NERDCollector(ObjectMapper jsonMapper, String appName, Properties properties) {
        super(jsonMapper, appName, properties);
        _httpTimeout = Duration.ofSeconds(Integer.parseInt(
                properties.getProperty(CollectorConfig.NERD_HTTP_TIMEOUT_CONFIG,
                        CollectorConfig.NERD_HTTP_TIMEOUT_DEFAULT)));
        _token = properties.getProperty(CollectorConfig.NERD_TOKEN_CONFIG, CollectorConfig.NERD_TOKEN_DEFAULT);
        _batchSize = Integer.parseInt(properties.getProperty(CollectorConfig.NERD_BATCH_SIZE_CONFIG,
                CollectorConfig.NERD_BATCH_SIZE_DEFAULT));

        _disabled = _token.isBlank();
        _executor = Executors.newVirtualThreadPerTaskExecutor();
    }

    @Override
    public void run(CommandLine cmd) {
        // Add a bit of a buffer to make the absolute processing timeout
        final var processingTimeout = (long) (_httpTimeout.toMillis() * 1.2);
        buildProcessor(_batchSize, processingTimeout);

        _client = HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.NEVER)
                .connectTimeout(_httpTimeout)
                .version(HttpClient.Version.HTTP_1_1)
                .executor(_executor)
                .build();


        // A batch counter for debugging only
        AtomicLong batchCounter = new AtomicLong(0);

        _parallelProcessor.subscribe(UniLists.of(Topics.IN_IP));
        _parallelProcessor.poll(ctx -> {
            var entries = ctx.streamConsumerRecords()
                    .filter(record -> {
                        final var request = record.value();
                        return request == null || request.collectors() == null
                                || request.collectors().contains(NAME);
                    })
                    .map(ConsumerRecord::key)
                    .toList();

            if (_disabled) {
                sendAboutAll(entries, errorResult(ResultCodes.DISABLED, null));
                return;
            }

            final var batch = batchCounter.getAndIncrement();
            Logger.trace("Processing batch {}: {}", batch, entries);

            // Bounded the processing with the absolute processing timeout
            var processFuture = this.processIps(entries, batch)
                    .orTimeout(processingTimeout, TimeUnit.MILLISECONDS);

            try {
                processFuture.join();
            } catch (CompletionException e) {
                if (e.getCause() instanceof TimeoutException) {
                    Logger.debug("Operation timed out (batch {})", batch);
                    sendAboutAll(entries, errorResult(ResultCodes.TIMEOUT,
                            "Operation timed out (%d ms)".formatted(processingTimeout)));
                } else {
                    Logger.warn("Unexpected error (batch {})", batch, e.getCause());
                    sendAboutAll(entries, errorResult(ResultCodes.INTERNAL_ERROR, e.getMessage()));
                }
            }
        });
    }

    private CompletableFuture<Void> processIps(List<IPToProcess> entries, final long batch) {
        var addressesToProcess = new ArrayList<Inet4Address>(entries.size());
        var entriesToProcess = new ArrayList<IPToProcess>(entries.size());
        // Filter out invalid or IPv6 addresses
        for (var inputIpToProcess : entries) {
            try {
                final var ip = InetAddresses.forString(inputIpToProcess.ip());
                if (ip instanceof Inet4Address ip4) {
                    addressesToProcess.add(ip4);
                    entriesToProcess.add(inputIpToProcess);
                    continue;
                } else if (ip instanceof Inet6Address) {
                    Logger.trace("Discarding IPv6 address: {}", inputIpToProcess.ip());
                    _producer.send(resultRecord(Topics.OUT_IP, inputIpToProcess,
                            errorResult(ResultCodes.UNSUPPORTED_ADDRESS, null)));
                    continue;
                }
            } catch (IllegalArgumentException e) {
                // Invalid IP address
            }
            Logger.debug("Invalid IP address: {}", inputIpToProcess);
            _producer.send(resultRecord(Topics.OUT_IP, inputIpToProcess,
                    errorResult(ResultCodes.INVALID_ADDRESS, null)));
        }

        if (addressesToProcess.isEmpty()) {
            Logger.trace("No IPs left in batch {}", batch);
            return CompletableFuture.completedFuture(null);
        }

        // Store the IPv4 addresses one after another in a byte array
        final var ipsLen = addressesToProcess.size();
        var bytes = new byte[ipsLen * 4];
        var ptr = 0;
        for (var ip : addressesToProcess) {
            System.arraycopy(ip.getAddress(), 0, bytes, ptr, 4);
            ptr += 4;
        }

        var listRequest = HttpRequest.newBuilder()
                .uri(URI.create(NERD_BASE + "ip/bulk/"))
                .timeout(_httpTimeout)
                .header("Content-Type", "application/octet-stream")
                .header("Authorization", _token)
                .header("Accept", "*/*")
                .POST(HttpRequest.BodyPublishers.ofByteArray(bytes))
                .build();

        Logger.trace("Sending NERD request (batch {})", batch);
        return _client.sendAsync(listRequest, HttpResponse.BodyHandlers.ofByteArray())
                .thenAccept(response -> {
                    if (response.statusCode() == 200) {
                        var resultData = response.body();
                        if (resultData.length % 8 != 0 || resultData.length / 8 != ipsLen) {
                            Logger.info("Invalid NERD response (batch {})", batch);
                            sendAboutAll(entriesToProcess, errorResult(ResultCodes.INVALID_RESPONSE,
                                    "Invalid NERD response (content length mismatch)"));
                            return;
                        }

                        Logger.trace("Processing {} IPs (batch {})", ipsLen, batch);
                        // Read the response data as doubles stored in little-endian order
                        var resultDataBuffer = ByteBuffer.wrap(resultData);
                        resultDataBuffer.order(ByteOrder.LITTLE_ENDIAN);

                        for (var i = 0; i < ipsLen; i++) {
                            var value = resultDataBuffer.getDouble(i);

                            Logger.trace("DN/IP {} -> {}", entriesToProcess.get(i), value);
                            _producer.send(resultRecord(Topics.OUT_IP, entriesToProcess.get(i),
                                    successResult(new NERDData(value))));
                        }
                    } else {
                        Logger.debug("NERD response {} (batch {})", response.statusCode(), batch);
                        sendAboutAll(entriesToProcess, errorResult(ResultCodes.CANNOT_FETCH,
                                "NERD response " + response.statusCode()));
                    }
                })
                .exceptionally(e -> {
                    if (e.getCause() instanceof HttpConnectTimeoutException) {
                        Logger.debug("Connection timeout (batch {})", batch, e);
                        sendAboutAll(entries, errorResult(ResultCodes.TIMEOUT,
                                "Connection timed out (%d ms)".formatted(_httpTimeout.toMillis())));
                    } else if (e.getCause() instanceof IOException) {
                        Logger.debug("I/O exception (batch {})", batch, e);
                        sendAboutAll(entries, errorResult(ResultCodes.CANNOT_FETCH, e.getMessage()));
                    } else {
                        Logger.warn("Unexpected error (batch {})", batch, e);
                        sendAboutAll(entries, errorResult(ResultCodes.INTERNAL_ERROR, e.getMessage()));
                    }
                    return null;
                });
    }

    private void sendAboutAll(List<IPToProcess> entries, CommonIPResult<NERDData> result) {
        sendAboutAll(_producer, Topics.OUT_IP, entries, result);
    }

    @Override
    public @NotNull String getName() {
        return NAME;
    }

    protected void buildProcessor(int batchSize, long timeoutMs) {
        _parallelProcessor = ParallelStreamProcessor.createEosStreamProcessor(
                this.buildProcessorOptions(batchSize, timeoutMs)
        );
    }

    @Override
    public void close() throws IOException {
        super.close();

        if (_parallelProcessor != null) {
            _client.close();
        }

        _executor.close();
    }
}