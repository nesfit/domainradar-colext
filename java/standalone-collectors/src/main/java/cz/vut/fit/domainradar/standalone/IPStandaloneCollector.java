package cz.vut.fit.domainradar.standalone;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import cz.vut.fit.domainradar.models.IPToProcess;
import cz.vut.fit.domainradar.models.ResultCodes;
import cz.vut.fit.domainradar.models.requests.IPRequest;
import cz.vut.fit.domainradar.models.results.CommonIPResult;
import cz.vut.fit.domainradar.serialization.JsonSerde;
import io.confluent.parallelconsumer.ParallelConsumer;
import io.confluent.parallelconsumer.ParallelStreamProcessor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.time.Instant;
import java.util.Properties;

/**
 * An abstract class for standalone collectors that process IP-related data.
 * This class extends the {@link BaseStandaloneCollector} and provides additional functionality
 * specific to IP processing and result handling.
 *
 * @param <TData> The type of the data to be processed and included in the result.
 * @author Ondřej Ondryáš
 * @see CommonIPResult
 */
public abstract class IPStandaloneCollector<TData, TProcessor extends ParallelConsumer<IPToProcess, IPRequest>>
        extends BaseStandaloneCollector<IPToProcess, IPRequest, TProcessor> {

    protected final KafkaProducer<IPToProcess, CommonIPResult<TData>> _producer;

    public IPStandaloneCollector(@NotNull ObjectMapper jsonMapper,
                                 @NotNull String appName,
                                 @NotNull Properties properties) {
        super(jsonMapper, appName, properties,
                JsonSerde.of(jsonMapper, IPToProcess.class),
                JsonSerde.of(jsonMapper, IPRequest.class));

        _producer = super.createProducer(JsonSerde.of(jsonMapper, IPToProcess.class).serializer(),
                JsonSerde.of(jsonMapper, new TypeReference<CommonIPResult<TData>>() {
                }).serializer());
    }

    /**
     * Creates an erroneous result.
     *
     * @param code    The error result code.
     * @param message The error message.
     * @return A {@link CommonIPResult} instance with the specified error code and message.
     */
    protected CommonIPResult<TData> errorResult(int code, String message) {
        return new CommonIPResult<>(code, message, Instant.now(), getName(), null);
    }

    /**
     * Creates a successful result.
     *
     * @param data The data to include in the result.
     * @return A {@link CommonIPResult} instance with the specified data.
     */
    protected CommonIPResult<TData> successResult(TData data) {
        return new CommonIPResult<>(ResultCodes.OK, null, Instant.now(), getName(), data);
    }

    @Override
    public void close() throws IOException {
        super.close();
        _producer.close(_closeTimeout);
    }
}
