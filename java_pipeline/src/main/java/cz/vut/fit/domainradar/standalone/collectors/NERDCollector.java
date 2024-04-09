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
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jetbrains.annotations.NotNull;
import pl.tlinkowski.unij.api.UniLists;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class NERDCollector extends IPStandaloneCollector<NERDData> {
    private static final String NERD_BASE = "https://nerd.cesnet.cz/nerd/api/v1/";

    private final Duration _httpTimeout;
    private final String _token;

    private HttpClient _client;
    private ExecutorService _executor;

    public NERDCollector(ObjectMapper jsonMapper, String appName, Properties properties) {
        super(jsonMapper, appName, properties);

        _properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        _httpTimeout = Duration.ofSeconds(Integer.parseInt(
                properties.getProperty(CollectorConfig.NERD_HTTP_TIMEOUT_CONFIG,
                        CollectorConfig.NERD_HTTP_TIMEOUT_DEFAULT)));
        _token = properties.getProperty(CollectorConfig.NERD_TOKEN_CONFIG, CollectorConfig.NERD_TOKEN_DEFAULT);

        if (_token.isBlank())
            throw new IllegalArgumentException("NERD token is not set.");
    }

    @Override
    public void run(CommandLine cmd) {
        buildProcessor(20);

        final var executor = _executor = Executors.newVirtualThreadPerTaskExecutor();
        _client = HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.NEVER)
                .connectTimeout(_httpTimeout)
                .version(HttpClient.Version.HTTP_1_1)
                .executor(executor)
                .build();

        _parallelProcessor.subscribe(UniLists.of(Topics.IN_IP));
        _parallelProcessor.poll(ctx -> {
            var entries = ctx.streamConsumerRecords().map(ConsumerRecord::key).toList();
            this.processIps(entries);
        });
    }

    private void processIps(List<IPToProcess> entries) {
        var ips = entries.stream().map(IPToProcess::ip).toList();
        var bytes = new byte[ips.size() * 4];
        var ptr = 0;
        for (var ip : ips) {
            var inetAddr = InetAddresses.forString(ip);
            if (!(inetAddr instanceof Inet4Address))
                continue;

            System.arraycopy(inetAddr.getAddress(), 0, bytes, ptr, 4);
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

        _client.sendAsync(listRequest, HttpResponse.BodyHandlers.ofByteArray())
                .thenAccept(response -> {
                    if (response.statusCode() == 200) {
                        var resultData = response.body();
                        if (resultData.length % 8 != 0 || resultData.length / 8 != ips.size()) {
                            sendAboutAll(entries, errorResult(ResultCodes.INVALID_FORMAT, "Invalid NERD response (content length mismatch)"));
                            return;
                        }

                        System.err.println("Processing " + ips.size() + " IPs from NERD");
                        var resultDataBuffer = ByteBuffer.wrap(resultData);
                        resultDataBuffer.order(ByteOrder.LITTLE_ENDIAN);

                        for (var i = 0; i < ips.size(); i++) {
                            var value = resultDataBuffer.getDouble(i);
                            _producer.send(new ProducerRecord<>(Topics.OUT_IP, entries.get(i),
                                    successResult(new NERDData(value))));
                        }
                    } else {
                        sendAboutAll(entries, errorResult(ResultCodes.CANNOT_FETCH, "NERD response " + response.statusCode()));

                    }
                })
                .exceptionally(e -> {
                    sendAboutAll(entries, errorResult(ResultCodes.CANNOT_FETCH, "Cannot fetch NERD data: " + e.getMessage()));
                    return null;
                });
    }

    private void sendAboutAll(List<IPToProcess> entries, CommonIPResult<NERDData> result) {
        sendAboutAll(Topics.OUT_IP, entries, result);
    }

    @Override
    public @NotNull String getName() {
        return "nerd";
    }

    @Override
    public void close() {
        super.close();

        if (_parallelProcessor != null) {
            _client.close();
            _executor.close();
        }
    }
}