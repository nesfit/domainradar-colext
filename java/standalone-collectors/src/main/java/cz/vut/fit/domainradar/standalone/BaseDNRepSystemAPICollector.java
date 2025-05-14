package cz.vut.fit.domainradar.standalone;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.vut.fit.domainradar.Topics;
import cz.vut.fit.domainradar.models.DNToProcess;
import cz.vut.fit.domainradar.models.ResultCodes;
import cz.vut.fit.domainradar.models.requests.DNRequest;
import io.confluent.parallelconsumer.ParallelStreamProcessor;
import org.apache.commons.cli.CommandLine;
import org.jetbrains.annotations.NotNull;
import org.json.JSONObject;
import pl.tlinkowski.unij.api.UniLists;

import java.time.Duration;
import java.util.Properties;

/**
 * An abstract base class for standalone collectors that consume Kafka records containing domain name to be processed
 * and produce Kafka records containing collected data about the domain name using an API.
 *
 * @param <TData> The type of data produced by the collector.
 * @author Matěj Čech
 */
public abstract class BaseDNRepSystemAPICollector<TData>
        extends DNStandaloneCollector<TData, ParallelStreamProcessor<DNToProcess, DNRequest>> {

    private final RepSystemAPIClient<DNToProcess, TData> _apiClient;
    private final Duration _httpTimeout;

    public BaseDNRepSystemAPICollector(ObjectMapper jsonMapper, String appName, Properties properties, String authToken,
                                       String timeoutConfig, String timeoutDefault) {
        super(jsonMapper, appName, properties);

        _httpTimeout = Duration.ofSeconds(Integer.parseInt(
                properties.getProperty(timeoutConfig, timeoutDefault)));

        _apiClient = new RepSystemAPIClient<>(authToken, _httpTimeout);
    }

    /**
     * Runs the collector and executes the processing logic for consuming and gathering data about domain names
     * from a Kafka topic.
     *
     * @param cmd The parsed command line arguments.
     */
    @Override
    public void run(CommandLine cmd) {
        // Add a bit of a buffer to make the absolute processing timeout
        final var processingTimeout = (long) (_httpTimeout.toMillis() * 1.2);

        buildProcessor(0, processingTimeout);

        // Subscribe to the parallel processor for the IN_DN topic
        _parallelProcessor.subscribe(UniLists.of(Topics.IN_DN));

        // Start polling for messages from the subscribed topic
        _parallelProcessor.poll(ctx -> {
            // Retrieve a single record from the consumer
            var entry = ctx.getSingleConsumerRecord();
            var dn = entry.key();

            _apiClient.execute(
                    dn,
                    this::getRequestUrl,
                    this::getUrlEncodedData,
                    this::getPOSTData,
                    getAuthTokenHeaderName(),
                    getLogger(),
                    (input, result) -> _producer.send(resultRecord(Topics.OUT_DN, input, successResult(result))),
                    (input, errorCode, errorMsg) -> _producer.send(resultRecord(Topics.OUT_DN, input,
                            errorResult(errorCode, errorMsg))),
                    this::mapResponseToData,
                    getCollectorName(),
                    processingTimeout
            );
        });
    }

    /**
     * Builds the parallel stream processor.
     *
     * @param batchSize The batch size for processing records.
     * @param timeoutMs The base timeout guaranteed by the collection process, in milliseconds.
     */
    protected void buildProcessor(int batchSize, long timeoutMs) {
        _parallelProcessor = ParallelStreamProcessor.createEosStreamProcessor(
                this.buildProcessorOptions(batchSize, timeoutMs)
        );
    }

    /**
     * Gets the Logger defined by its child class.
     *
     * @return The logger instance used for logging messages from the specific collector.
     */
    protected abstract org.slf4j.Logger getLogger();

    /**
     * Builds the request URL for the given domain name.
     *
     * @param dn The domain name to process, which will be used to generate the request URL.
     * @return The constructed request URL for the given domain name.
     */
    protected abstract String getRequestUrl(DNToProcess dn);

    /**
     * Gets the name of the HTTP header that should contain the authentication token for a given service the collector
     * uses.
     *
     * @return The name of the authentication token header or null if not required.
     */
    protected abstract String getAuthTokenHeaderName();

    /**
     * Gets the collector's name.
     *
     * @return The name of the collector.
     */
    protected abstract String getCollectorName();

    /**
     * Maps the JSON response from the API to the TData data type.
     *
     * @param jsonResponse The JSON response received from the API.
     * @return The mapped data in the TData data type.
     */
    protected abstract TData mapResponseToData(JSONObject jsonResponse);

    /**
     * Optionally provides URL-encoded data for the request.
     * By default, this method returns null, but it can be overridden by a subclass to provide URL-encoded data
     * for POST requests if needed.
     *
     * @param dn The domain name to be processed.
     * @return The URL-encoded data as a string, or null if no URL-encoded data is required.
     */
    protected String getUrlEncodedData(DNToProcess dn) {
        return null;
    }

    /**
     * Optionally provides POST data for the request.
     * By default, this method returns null, but it can be overridden by a subclass to provide POST data if needed.
     *
     * @param dn The domain name to be processed.
     * @return The POST data as a string, or null if no POST data is required.
     */
    protected String getPOSTData(DNToProcess dn) {
        return null;
    }

    /**
     * Gets the name of the collector.
     *
     * @return The name of the collector.
     */
    @Override
    public @NotNull String getName() {
        return getCollectorName();
    }
}
