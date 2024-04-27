package cz.vut.fit.domainradar.standalone;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import cz.vut.fit.domainradar.models.IPToProcess;
import cz.vut.fit.domainradar.models.ResultCodes;
import cz.vut.fit.domainradar.models.requests.IPProcessRequest;
import cz.vut.fit.domainradar.models.results.CommonIPResult;
import cz.vut.fit.domainradar.serialization.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;
import java.util.Properties;

public abstract class IPStandaloneCollector<TData> extends BaseStandaloneCollector<IPToProcess, IPProcessRequest,
        IPToProcess, CommonIPResult<TData>> {
    public IPStandaloneCollector(@NotNull ObjectMapper jsonMapper,
                                 @NotNull String appName,
                                 @Nullable Properties properties) {
        super(jsonMapper, appName, properties,
                JsonSerde.of(jsonMapper, IPToProcess.class),
                JsonSerde.of(jsonMapper, IPToProcess.class),
                JsonSerde.of(jsonMapper, IPProcessRequest.class),
                JsonSerde.of(jsonMapper, new TypeReference<>() {
                }));
    }

    protected CommonIPResult<TData> errorResult(int code, String message) {
        return new CommonIPResult<>(code, message, Instant.now(), getName(), null);
    }

    protected CommonIPResult<TData> successResult(TData data) {
        return new CommonIPResult<>(ResultCodes.OK, null, Instant.now(), getName(), data);
    }
}
