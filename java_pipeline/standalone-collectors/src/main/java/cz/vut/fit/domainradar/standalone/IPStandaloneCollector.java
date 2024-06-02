package cz.vut.fit.domainradar.standalone;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import cz.vut.fit.domainradar.models.IPToProcess;
import cz.vut.fit.domainradar.models.ResultCodes;
import cz.vut.fit.domainradar.models.requests.IPRequest;
import cz.vut.fit.domainradar.models.results.CommonIPResult;
import cz.vut.fit.domainradar.serialization.JsonSerde;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;
import java.util.Properties;

public abstract class IPStandaloneCollector<TData> extends BaseStandaloneCollector<IPToProcess, IPRequest> {

    protected KafkaProducer<IPToProcess, CommonIPResult<TData>> _producer;

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

    protected CommonIPResult<TData> errorResult(int code, String message) {
        return new CommonIPResult<>(code, message, Instant.now(), getName(), null);
    }

    protected CommonIPResult<TData> successResult(TData data) {
        return new CommonIPResult<>(ResultCodes.OK, null, Instant.now(), getName(), data);
    }
}
