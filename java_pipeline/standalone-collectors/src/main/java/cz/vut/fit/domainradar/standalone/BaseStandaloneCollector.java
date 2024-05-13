package cz.vut.fit.domainradar.standalone;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.vut.fit.domainradar.CollectorConfig;
import cz.vut.fit.domainradar.models.results.Result;
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
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;

public abstract class BaseStandaloneCollector<KIn, VIn, KOut, VOut extends Result> implements Closeable {

    protected final Properties _properties;
    protected final Duration _closeTimeout;
    private final Duration _commitInterval;
    private final int _maxConcurrency;

    protected final ObjectMapper _jsonMapper;
    protected final KafkaConsumer<KIn, VIn> _consumer;
    protected final KafkaProducer<KOut, VOut> _producer;
    protected ParallelStreamProcessor<KIn, VIn> _parallelProcessor;

    public BaseStandaloneCollector(@NotNull ObjectMapper jsonMapper,
                                   @NotNull String appName,
                                   @Nullable Properties properties,
                                   @NotNull Serde<KIn> keyInSerde,
                                   @NotNull Serde<KOut> keyOutSerde,
                                   @NotNull Serde<VIn> valueInSerde,
                                   @NotNull Serde<VOut> valueOutSerde) {
        _jsonMapper = jsonMapper;

        var groupId = appName + "-" + getName();

        _properties = new Properties();
        if (properties != null)
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
        _producer = createProducer(keyOutSerde.serializer(), valueOutSerde.serializer());
    }

    public abstract void run(CommandLine cmd);

    public abstract @NotNull String getName();

    protected void sendAboutAll(@NotNull String topic, @NotNull List<KOut> entries, @NotNull VOut result) {
        for (var entry : entries) {
            _producer.send(new ProducerRecord<>(topic, entry, result));
        }
    }

    protected void send(@NotNull String topic, KOut key, VOut value) {
        _producer.send(new ProducerRecord<>(topic, key, value));
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

    protected @NotNull KafkaProducer<KOut, VOut> createProducer(@NotNull Serializer<KOut> keySerializer,
                                                                @NotNull Serializer<VOut> valueSerializer) {
        var properties = new Properties();
        properties.putAll(_properties);
        properties.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "producer-" + getName() + "-A");
        
        return new KafkaProducer<>(properties, keySerializer, valueSerializer);
    }

    public void addOptions(@NotNull Options options) {
    }

    @Override
    public void close() {
        if (_parallelProcessor != null) {
            _parallelProcessor.close(_closeTimeout, DrainingCloseable.DrainingMode.DRAIN);
        }

        _consumer.close(_closeTimeout);
        _producer.close(_closeTimeout);
    }

    protected VOut errorResult(int code, @NotNull String message, @NotNull Class<?> clz, Object[]... args) {
        try {
            final var constructor = clz.getDeclaredConstructors()[0];
            Object[] parValues = new Object[constructor.getParameterCount()];

            parValues[0] = code;
            parValues[1] = message;
            parValues[2] = Instant.now();
            if (args != null && args.length > 0 && args.length <= parValues.length - 3) {
                System.arraycopy(args, 0, parValues, 3, args.length);
            }

            //noinspection unchecked
            return (VOut) constructor.newInstance(parValues);
        } catch (Exception constructorException) {
            throw new RuntimeException(constructorException);
        }
    }
}
