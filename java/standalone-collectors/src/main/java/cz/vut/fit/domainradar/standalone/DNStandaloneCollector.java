package cz.vut.fit.domainradar.standalone;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import cz.vut.fit.domainradar.models.DNToProcess;
import cz.vut.fit.domainradar.models.ResultCodes;
import cz.vut.fit.domainradar.models.requests.DNRequest;
import cz.vut.fit.domainradar.models.results.CommonDNResult;
import cz.vut.fit.domainradar.serialization.JsonSerde;
import io.confluent.parallelconsumer.ParallelConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.time.Instant;
import java.util.Properties;

/**
 * An abstract class for standalone collectors that process DN-related data.
 * This class extends the {@link BaseStandaloneCollector} and provides additional functionality
 * specific to DN processing and result handling.
 *
 * @param <TData> The type of the data to be processed and included in the result.
 * @author Matěj Čech
 * @see CommonDNResult
 */
public abstract class DNStandaloneCollector<TData, TProcessor extends ParallelConsumer<DNToProcess, DNRequest>>
        extends BaseStandaloneCollector<DNToProcess, DNRequest, TProcessor> {
    protected final KafkaProducer<DNToProcess, CommonDNResult<TData>> _producer;

    public DNStandaloneCollector(@NotNull ObjectMapper jsonMapper,
                                 @NotNull String appName,
                                 @NotNull Properties properties) {
        super(jsonMapper, appName, properties,
                JsonSerde.of(jsonMapper, DNToProcess.class),
                JsonSerde.of(jsonMapper, DNRequest.class));

        _producer = super.createProducer(JsonSerde.of(jsonMapper, DNToProcess.class).serializer(),
                JsonSerde.of(jsonMapper, new TypeReference<CommonDNResult<TData>>() {
                }).serializer());
    }

    /**
     * Creates an erroneous result.
     *
     * @param code    The error result code.
     * @param message The error message.
     * @return A {@link CommonDNResult} instance with the specific error code and message.
     */
    protected CommonDNResult<TData> errorResult(int code, String message) {
        return new CommonDNResult<>(code, message, Instant.now(), getName(), null);
    }

    /**
     * Creates a successful result.
     *
     * @param data The data to include in the result.
     * @return A {@link CommonDNResult} instance with the specific data.
     */
    protected CommonDNResult<TData> successResult(TData data) {
        return new CommonDNResult<>(ResultCodes.OK, null, Instant.now(), getName(), data);
    }

    @Override
    public void close() throws IOException {
        super.close();
        _producer.close(_closeTimeout);
    }
}
