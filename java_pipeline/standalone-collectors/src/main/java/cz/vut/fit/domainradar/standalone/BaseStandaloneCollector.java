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

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;

/**
 * An abstract base class for standalone collectors that consume and process Kafka records.
 * This class provides common functionality for managing Kafka consumers, producers, and parallel processing
 * using Confluent Parallel Consumer.
 *
 * @param <KIn> The type of the key for input Kafka records.
 * @param <VIn> The type of the value for input Kafka records.
 * @author Ondřej Ondryáš
 */
public abstract class BaseStandaloneCollector<KIn, VIn> implements Closeable {

    protected final Properties _properties;
    protected final Duration _closeTimeout;
    private final Duration _commitInterval;
    private final int _maxConcurrency;
    private final ParallelConsumerOptions.CommitMode _commitMode;

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
        // The CPC library requires disabling auto-commit as it manages the offsets itself
        _properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        _properties.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "60000");

        _maxConcurrency = Integer.parseInt(_properties.getProperty(
                CollectorConfig.MAX_CONCURRENCY_CONFIG, CollectorConfig.MAX_CONCURRENCY_DEFAULT));
        _closeTimeout = Duration.ofSeconds(Long.parseLong(_properties.getProperty(
                CollectorConfig.CLOSE_TIMEOUT_SEC_CONFIG, CollectorConfig.CLOSE_TIMEOUT_SEC_DEFAULT)));
        _commitInterval = Duration.ofMillis(Long.parseLong(_properties.getProperty(
                CollectorConfig.COMMIT_INTERVAL_MS_CONFIG, CollectorConfig.COMMIT_INTERVAL_MS_DEFAULT)));
        _commitMode = ParallelConsumerOptions.CommitMode.valueOf(_properties.getProperty(
                CollectorConfig.COMMIT_MODE_CONFIG, CollectorConfig.COMMIT_MODE_DEFAULT));

        _consumer = createConsumer(keyInSerde.deserializer(), valueInSerde.deserializer());
    }

    /**
     * Runs the collector with the specified command line arguments.
     * The implementations must use it to call the parallel processor's {@link ParallelStreamProcessor#poll(Consumer)} method
     * and return, without blocking.
     *
     * @param cmd The parsed command line arguments.
     */
    public abstract void run(CommandLine cmd);

    /**
     * Returns the collector identifier.
     * <p>
     * This is used, for example, to populate the
     * {@link cz.vut.fit.domainradar.models.results.CommonIPResult#collector()} field in IP results.
     *
     * @return The collector identifier.
     */
    public abstract @NotNull String getName();

    /**
     * Sends multiple copies of the same result under multiple keys to a given topic.
     *
     * @param producer The producer to use for sending the results.
     * @param topic    The target topic.
     * @param entries  The list of keys to send the result under.
     * @param result   The result to send.
     * @param <KOut>   The type of the keys to send.
     * @param <VOut>   The type of the result to send.
     */
    protected static <KOut, VOut extends Result> void sendAboutAll(@NotNull KafkaProducer<KOut, VOut> producer,
                                                                   @NotNull String topic, @NotNull List<KOut> entries,
                                                                   @NotNull VOut result) {
        for (var entry : entries) {
            producer.send(new ProducerRecord<>(topic, null, result.lastAttempt().toEpochMilli(), entry, result));
        }
    }

    /**
     * Creates a {@link ProducerRecord} for a given topic, key, and {@link Result}-like value. Uses the
     * result's {@link Result#lastAttempt()} as the timestamp for the produced Kafka record.
     *
     * @param topic  The target topic.
     * @param key    The key for the produced record.
     * @param value  The value for the produced record.
     * @param <KOut> The type of the key to send.
     * @param <VOut> The type of the result to send. Extends {@link Result}.
     * @return The Kafka record.
     */
    protected static <KOut, VOut extends Result> ProducerRecord<KOut, VOut> resultRecord(
            String topic, KOut key, VOut value) {
        return new ProducerRecord<>(topic, null, value.lastAttempt().toEpochMilli(), key, value);
    }

    /**
     * Builds the ParallelStreamProcessor with the specified batch size.
     * Uses the properties loaded in the constructor.
     *
     * @param batchSize The batch size for processing records. If greater than 0,
     *                  the processor will be configured to use the specified batch size.
     * @param timeoutMs The base timeout guaranteed by the collection process, in milliseconds.
     *                  This value will be multiplied by a small coefficient to set the threshold for
     *                  time spent in the queue of the parallel consumer before issuing a warning.
     */
    protected void buildProcessor(int batchSize, long timeoutMs) {
        var optionsBuilder = ParallelConsumerOptions.<KIn, VIn>builder()
                .ordering(ParallelConsumerOptions.ProcessingOrder.KEY)
                .consumer(_consumer)
                .maxConcurrency(_maxConcurrency)
                .thresholdForTimeSpendInQueueWarning(Duration.ofMillis((long)(timeoutMs * 1.1)))
                .commitMode(_commitMode)
                .commitInterval(_commitInterval);

        if (batchSize > 0)
            optionsBuilder = optionsBuilder.batchSize(batchSize);

        _parallelProcessor = ParallelStreamProcessor.createEosStreamProcessor(optionsBuilder.build());
    }

    /**
     * Creates a Kafka consumer using the properties loaded in the constructor.
     *
     * @param keyDeserializer   The key deserializer to use.
     * @param valueDeserializer The value deserializer to use.
     * @return The created Kafka consumer.
     */
    protected @NotNull KafkaConsumer<KIn, VIn> createConsumer(@NotNull Deserializer<KIn> keyDeserializer,
                                                              @NotNull Deserializer<VIn> valueDeserializer) {
        return new KafkaConsumer<>(_properties, keyDeserializer, valueDeserializer);
    }

    /**
     * Creates a Kafka producer using the properties loaded in the constructor. The producer is assigned an identifier
     * composed of the string "producer-", the collector identifier, and the string "main".
     *
     * @param keySerializer   The key serializer to use.
     * @param valueSerializer The value serializer to use.
     * @param <KOut>          The type of the keys to serialize.
     * @param <VOut>          The type of the values to serialize.
     * @return The created Kafka producer.
     */
    protected @NotNull <KOut, VOut> KafkaProducer<KOut, VOut> createProducer(@NotNull Serializer<KOut> keySerializer,
                                                                             @NotNull Serializer<VOut> valueSerializer) {
        return createProducer(keySerializer, valueSerializer, "main");
    }

    /**
     * Creates a Kafka producer using the properties loaded in the constructor. The producer is assigned an identifier
     * composed of the string "producer-", the collector identifier, and the given discriminator value.
     *
     * @param keySerializer   The key serializer to use.
     * @param valueSerializer The value serializer to use.
     * @param discriminator   A discriminator to append to the client ID to distinguish between multiple producers.
     * @param <KOut>          The type of the keys to serialize.
     * @param <VOut>          The type of the values to serialize.
     * @return The created Kafka producer.
     */
    protected @NotNull <KOut, VOut> KafkaProducer<KOut, VOut> createProducer(@NotNull Serializer<KOut> keySerializer,
                                                                             @NotNull Serializer<VOut> valueSerializer,
                                                                             String discriminator) {
        var properties = new Properties();
        properties.putAll(_properties);
        properties.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "producer-" + getName() + "-" + discriminator);

        return new KafkaProducer<>(properties, keySerializer, valueSerializer);
    }

    /**
     * Adds additional command-line options specific to the collector.
     *
     * @param options The options object to add the options to.
     */
    public void addOptions(@NotNull Options options) {
    }

    /**
     * Closes the collector, stopping the parallel processor and the consumer.
     *
     * @throws IOException If an error occurs while closing the processor or the consumer.
     */
    @Override
    public void close() throws IOException {
        if (_parallelProcessor != null) {
            _parallelProcessor.close(_closeTimeout, DrainingCloseable.DrainingMode.DRAIN);
        }

        _consumer.close(_closeTimeout);
    }
}
