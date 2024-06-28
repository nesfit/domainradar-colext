package cz.vut.fit.domainradar.standalone.collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.net.InetAddresses;
import cz.vut.fit.domainradar.CollectorConfig;
import cz.vut.fit.domainradar.Topics;
import cz.vut.fit.domainradar.models.IPToProcess;
import cz.vut.fit.domainradar.models.ResultCodes;
import cz.vut.fit.domainradar.models.ip.NERDData;
import cz.vut.fit.domainradar.models.results.CommonIPResult;
import cz.vut.fit.domainradar.standalone.IPStandaloneCollector;
import org.apache.commons.cli.CommandLine;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jetbrains.annotations.NotNull;
import org.slf4j.LoggerFactory;
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
import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class NERDCollector extends IPStandaloneCollector<NERDData> {
    private static final org.slf4j.Logger Logger = LoggerFactory.getLogger(NERDCollector.class);

    public static final String NAME = "nerd";
    private static final String NERD_BASE = "https://nerd.cesnet.cz/nerd/api/v1/";

    private final ExecutorService _executor;
    private final String _token;
    private HttpClient _client;

    private final Duration _httpTimeout;
    private final int _batchSize;

    public NERDCollector(ObjectMapper jsonMapper, String appName, Properties properties) {
        super(jsonMapper, appName, properties);
        _httpTimeout = Duration.ofSeconds(Integer.parseInt(
                properties.getProperty(CollectorConfig.NERD_HTTP_TIMEOUT_CONFIG,
                        CollectorConfig.NERD_HTTP_TIMEOUT_DEFAULT)));
        _token = properties.getProperty(CollectorConfig.NERD_TOKEN_CONFIG, CollectorConfig.NERD_TOKEN_DEFAULT);
        _batchSize = Integer.parseInt(properties.getProperty(CollectorConfig.NERD_BATCH_SIZE_CONFIG,
                CollectorConfig.NERD_BATCH_SIZE_DEFAULT));

        if (_token.isBlank())
            throw new IllegalArgumentException("NERD token is not set.");

        _executor = Executors.newVirtualThreadPerTaskExecutor();
    }

    @Override
    public void run(CommandLine cmd) {
        buildProcessor(_batchSize);

        _client = HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.NEVER)
                .connectTimeout(_httpTimeout)
                .version(HttpClient.Version.HTTP_1_1)
                .executor(_executor)
                .build();

        final var processingTimeout = (long) (_httpTimeout.toMillis() * 1.2);

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

            final var batch = System.nanoTime() % 1000000;
            Logger.trace("Processing batch {}: {}", batch, entries);
            var processFuture = this.processIps(entries, batch)
                    .orTimeout(processingTimeout, TimeUnit.MILLISECONDS);

            try {
                processFuture.join();
            } catch (CompletionException e) {
                if (e.getCause() instanceof TimeoutException) {
                    sendAboutAll(entries, errorResult(ResultCodes.TIMEOUT,
                            "Operation timed out (%d ms)".formatted(processingTimeout)));
                } else {
                    sendAboutAll(entries, errorResult(ResultCodes.INTERNAL_ERROR, e.getMessage()));
                }
            }
        });
    }

    private CompletableFuture<Void> processIps(List<IPToProcess> entries, final long batch) {
        var allIps = entries.stream()
                .map(ipToProcess -> {
                    try {
                        return InetAddresses.forString(ipToProcess.ip());
                    } catch (IllegalArgumentException e) {
                        Logger.debug("Invalid IP address: {}", ipToProcess.ip());
                        return null;
                    }
                }).collect(Collectors.partitioningBy(ip -> ip instanceof Inet4Address));

        for (var invalidIp : allIps.get(false)) {
            if (invalidIp instanceof Inet6Address) {
                sendAboutAll(entries, errorResult(ResultCodes.UNSUPPORTED_ADDRESS, null));
            } else {
                sendAboutAll(entries, errorResult(ResultCodes.INVALID_ADDRESS, null));
            }
        }

        var ips = allIps.get(true);
        final var ipsLen = ips.size();
        var bytes = new byte[ipsLen * 4];
        var ptr = 0;
        for (var ip : ips) {
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

        return _client.sendAsync(listRequest, HttpResponse.BodyHandlers.ofByteArray())
                .thenAccept(response -> {
                    if (response.statusCode() == 200) {
                        var resultData = response.body();
                        if (resultData.length % 8 != 0 || resultData.length / 8 != ipsLen) {
                            Logger.info("Invalid NERD response (batch {})", batch);
                            sendAboutAll(entries, errorResult(ResultCodes.INVALID_RESPONSE,
                                    "Invalid NERD response (content length mismatch)"));
                            return;
                        }

                        Logger.trace("Processing {} IPs (batch {})", ipsLen, batch);
                        var resultDataBuffer = ByteBuffer.wrap(resultData);
                        resultDataBuffer.order(ByteOrder.LITTLE_ENDIAN);

                        for (var i = 0; i < ipsLen; i++) {
                            var value = resultDataBuffer.getDouble(i);

                            Logger.trace("DN/IP {} -> {}", entries.get(i), value);
                            _producer.send(new ProducerRecord<>(Topics.OUT_IP, entries.get(i),
                                    successResult(new NERDData(value))));
                        }
                    } else {
                        Logger.debug("NERD response {} (batch {})", response.statusCode(), batch);
                        sendAboutAll(entries, errorResult(ResultCodes.CANNOT_FETCH,
                                "NERD response " + response.statusCode()));
                    }
                })
                .exceptionally(e -> {
                    Logger.debug("Error processing response (batch {})", batch, e);
                    if (e.getCause() instanceof HttpConnectTimeoutException) {
                        sendAboutAll(entries, errorResult(ResultCodes.TIMEOUT,
                                "Connection timed out (%d ms)".formatted(_httpTimeout.toMillis())));
                    } else {
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

    @Override
    public void close() throws IOException {
        super.close();

        if (_parallelProcessor != null) {
            _client.close();
        }

        _executor.close();
    }
}