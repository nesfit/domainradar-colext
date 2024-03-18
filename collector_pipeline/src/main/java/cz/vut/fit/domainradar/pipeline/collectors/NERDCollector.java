package cz.vut.fit.domainradar.pipeline.collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.vut.fit.domainradar.CollectorConfig;
import cz.vut.fit.domainradar.models.StringPair;
import cz.vut.fit.domainradar.models.ip.NERDData;
import cz.vut.fit.domainradar.models.results.CommonIPResult;
import cz.vut.fit.domainradar.pipeline.CommonResultIPCollector;
import cz.vut.fit.domainradar.pipeline.ErrorCodes;
import cz.vut.fit.domainradar.serialization.JsonSerializer;
import cz.vut.fit.domainradar.serialization.StringPairSerde;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelStreamProcessor;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.VoidDeserializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
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

public class NERDCollector implements CommonResultIPCollector<NERDData> {
    public static final int CLOSE_TIMEOUT_S = 5;
    private static final String NERD_BASE = "https://nerd.cesnet.cz/nerd/api/v1/";
    private static final String RESULT_TOPIC = "collected_IP_data";

    private final ObjectMapper _jsonMapper;
    private final Properties _properties;
    private final Duration _httpTimeout;

    private Consumer<StringPair, Void> _kafkaConsumer;
    private Producer<StringPair, CommonIPResult<NERDData>> _kafkaProducer;
    private ParallelStreamProcessor<StringPair, Void> _parallelStreamProcessor;

    private HttpClient _client;
    private ExecutorService _executor;

    public NERDCollector(ObjectMapper jsonMapper, Properties properties) {
        _jsonMapper = jsonMapper;

        var appId = properties.getProperty(StreamsConfig.APPLICATION_ID_CONFIG);
        var groupId = appId + "-" + getName() + "-parallel-consumer-group";

        _properties = new Properties();
        _properties.putAll(properties);
        _properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        _properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        _httpTimeout = Duration.ofSeconds(Integer.parseInt(
                properties.getProperty(CollectorConfig.NERD_HTTP_TIMEOUT_CONFIG,
                        CollectorConfig.NERD_HTTP_TIMEOUT_DEFAULT)));
    }

    private void setupParallelConsumer() {
        var stringPairSerde = StringPairSerde.build();

        _kafkaConsumer =
                new KafkaConsumer<>(_properties, stringPairSerde.deserializer(), new VoidDeserializer());
        _kafkaProducer =
                new KafkaProducer<>(_properties, stringPairSerde.serializer(), new JsonSerializer<>(_jsonMapper));

        var options = ParallelConsumerOptions.<StringPair, Void>builder()
                .ordering(ParallelConsumerOptions.ProcessingOrder.KEY)
                .consumer(_kafkaConsumer)
                .maxConcurrency(32)
                .batchSize(30)
                .commitInterval(Duration.ofMillis(100))
                .build();

        final var parallelSp = _parallelStreamProcessor
                = ParallelStreamProcessor.createEosStreamProcessor(options);

        final var executor = _executor = Executors.newVirtualThreadPerTaskExecutor();
        _client = HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.NEVER)
                .connectTimeout(_httpTimeout)
                .version(HttpClient.Version.HTTP_1_1)
                .executor(executor)
                .build();

        parallelSp.subscribe(UniLists.of("to_process_IP"));
        parallelSp.poll(ctx -> {
            var entries = ctx.streamConsumerRecords().map(ConsumerRecord::key).toList();
            this.processIps(entries);
        });
    }

    private void processIps(List<StringPair> entries) {
        var ips = entries.stream().map(StringPair::ip).toList();
        var bytes = new byte[ips.size() * 4];
        var ptr = 0;
        for (var ip : ips) {
            try {
                var inetAddr = InetAddress.getByName(ip);
                if (!(inetAddr instanceof Inet4Address))
                    continue;

                System.arraycopy(inetAddr.getAddress(), 0, bytes, ptr, 4);
                ptr += 4;
            } catch (UnknownHostException e) {
                continue;
            }
        }

        var listRequest = HttpRequest.newBuilder()
                .uri(URI.create(NERD_BASE + "ip/bulk/"))
                .timeout(_httpTimeout)
                .header("Content-Type", "application/octet-stream")
                .header("Authorization", "token TOKENHERE")
                .header("Accept", "*/*")
                .POST(HttpRequest.BodyPublishers.ofByteArray(bytes))
                .build();

        _client.sendAsync(listRequest, HttpResponse.BodyHandlers.ofByteArray())
                .thenAccept(response -> {
                    if (response.statusCode() == 200) {
                        var resultData = response.body();
                        if (resultData.length % 8 != 0 || resultData.length / 8 != ips.size()) {
                            sendAboutAll(entries, errorResult("Invalid NERD response (content length mismatch)",
                                    ErrorCodes.INVALID_FORMAT));
                            return;
                        }

                        System.err.println("Processing " + ips.size() + " IPs from NERD");
                        var resultDataBuffer = ByteBuffer.wrap(resultData);
                        resultDataBuffer.order(ByteOrder.LITTLE_ENDIAN);

                        for (var i = 0; i < ips.size(); i++) {
                            var value = resultDataBuffer.getDouble(i);
                            _kafkaProducer.send(new ProducerRecord<>(RESULT_TOPIC, entries.get(i),
                                    successResult(new NERDData(value))));
                        }
                    } else {
                        sendAboutAll(entries, errorResult("NERD response " + response.statusCode(),
                                ErrorCodes.CANNOT_FETCH));

                    }
                })
                .exceptionally(e -> {
                    sendAboutAll(entries, errorResult("Cannot fetch NERD data: " + e.getMessage(),
                            ErrorCodes.CANNOT_FETCH));
                    return null;
                });
    }

    private void sendAboutAll(List<StringPair> entries, CommonIPResult<NERDData> result) {
        for (var entry : entries) {
            _kafkaProducer.send(new ProducerRecord<>(RESULT_TOPIC, entry, result));
        }
    }

    @Override
    public void use(StreamsBuilder builder) {
        setupParallelConsumer();
    }

    @Override
    public String getName() {
        return "COL_NERD";
    }

    @Override
    public String getCollectorName() {
        return "nerd";
    }

    @Override
    public boolean createsStreamTopology() {
        return false;
    }

    @Override
    public void close() {
        if (_parallelStreamProcessor != null) {
            _parallelStreamProcessor.closeDrainFirst(Duration.ofSeconds(CLOSE_TIMEOUT_S));
            _kafkaProducer.close(Duration.ofSeconds(CLOSE_TIMEOUT_S));
            _kafkaConsumer.close(Duration.ofSeconds(CLOSE_TIMEOUT_S));

            _client.close();
            _executor.close();
        }
    }
}