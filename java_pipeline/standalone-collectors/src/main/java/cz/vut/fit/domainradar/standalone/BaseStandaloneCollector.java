package cz.vut.fit.domainradar.standalone;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.vut.fit.domainradar.CollectorConfig;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelStreamProcessor;
import io.confluent.parallelconsumer.internal.DrainingCloseable;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

public abstract class BaseStandaloneCollector<KIn, VIn> implements Closeable {

    protected final Properties _properties;
    protected final Duration _closeTimeout;
    private final Duration _commitInterval;
    private final int _maxConcurrency;

    protected final ObjectMapper _jsonMapper;
    protected final KafkaConsumer<KIn, VIn> _consumer;
    protected ParallelStreamProcessor<KIn, VIn> _parallelProcessor;

    public BaseStandaloneCollector(@NotNull ObjectMapper jsonMapper,
                                   @NotNull String appName,
                                   @NotNull Properties properties,
                                   @NotNull Serde<KIn> keyInSerde,
                                   @NotNull Serde<VIn> valueInSerde) {
        _jsonMapper = jsonMapper;

        var groupId = appName + "-" + getName();

        _properties = new Properties();
        _properties.putAll(properties);

        _properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        _properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        _properties.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "60000");

        _maxConcurrency = Integer.parseInt(_properties.getProperty(
                CollectorConfig.MAX_CONCURRENCY_CONFIG, CollectorConfig.MAX_CONCURRENCY_DEFAULT));
        _closeTimeout = Duration.ofSeconds(Long.parseLong(_properties.getProperty(
                CollectorConfig.CLOSE_TIMEOUT_SEC_CONFIG, CollectorConfig.CLOSE_TIMEOUT_SEC_DEFAULT)));
        _commitInterval = Duration.ofMillis(Long.parseLong(_properties.getProperty(
                CollectorConfig.COMMIT_INTERVAL_MS_CONFIG, CollectorConfig.COMMIT_INTERVAL_MS_DEFAULT)));

        _consumer = createConsumer(keyInSerde.deserializer(), valueInSerde.deserializer());
    }

    public abstract void run(CommandLine cmd);

    public abstract @NotNull String getName();

    protected static <KOut, VOut> void sendAboutAll(@NotNull KafkaProducer<KOut, VOut> producer,
                                                    @NotNull String topic, @NotNull List<KOut> entries,
                                                    @NotNull VOut result) {
        for (var entry : entries) {
            producer.send(new ProducerRecord<>(topic, entry, result));
        }
    }

    protected void buildProcessor(int batchSize) {
        var optionsBuilder = ParallelConsumerOptions.<KIn, VIn>builder()
                .ordering(ParallelConsumerOptions.ProcessingOrder.KEY)
                .consumer(_consumer)
                .maxConcurrency(_maxConcurrency)
                .commitInterval(_commitInterval);

        if (batchSize > 0)
            optionsBuilder = optionsBuilder.batchSize(batchSize);

        _parallelProcessor = ParallelStreamProcessor.createEosStreamProcessor(optionsBuilder.build());
    }

    protected @NotNull KafkaConsumer<KIn, VIn> createConsumer(@NotNull Deserializer<KIn> keyDeserializer,
                                                              @NotNull Deserializer<VIn> valueDeserializer) {
        return new KafkaConsumer<>(_properties, keyDeserializer, valueDeserializer);
    }

    protected @NotNull <KOut, VOut> KafkaProducer<KOut, VOut> createProducer(@NotNull Serializer<KOut> keySerializer,
                                                                             @NotNull Serializer<VOut> valueSerializer) {
        return createProducer(keySerializer, valueSerializer, "A");
    }

    protected @NotNull <KOut, VOut> KafkaProducer<KOut, VOut> createProducer(@NotNull Serializer<KOut> keySerializer,
                                                                             @NotNull Serializer<VOut> valueSerializer,
                                                                             String discriminator) {
        var properties = new Properties();
        properties.putAll(_properties);
        properties.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "producer-" + getName() + "-" + discriminator);

        return new KafkaProducer<>(properties, keySerializer, valueSerializer);
    }

    public void addOptions(@NotNull Options options) {
    }

    @Override
    public void close() throws IOException {
        if (_parallelProcessor != null) {
            _parallelProcessor.close(_closeTimeout, DrainingCloseable.DrainingMode.DRAIN);
        }

        _consumer.close(_closeTimeout);
    }
}
