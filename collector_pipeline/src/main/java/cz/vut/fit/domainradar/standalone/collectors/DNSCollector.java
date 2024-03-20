package cz.vut.fit.domainradar.standalone.collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.vut.fit.domainradar.models.requests.DNSProcessRequest;
import cz.vut.fit.domainradar.models.results.DNSResult;
import cz.vut.fit.domainradar.serialization.JsonSerde;
import cz.vut.fit.domainradar.standalone.BaseStandaloneCollector;
import org.apache.commons.cli.CommandLine;
import org.apache.kafka.common.serialization.Serdes;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Properties;

public class DNSCollector extends BaseStandaloneCollector<String, DNSProcessRequest, String, DNSResult> {
    public DNSCollector(@NotNull ObjectMapper jsonMapper,
                        @NotNull String appName,
                        @Nullable Properties properties) {
        super(jsonMapper, appName, properties,
                Serdes.String(),
                Serdes.String(),
                JsonSerde.of(jsonMapper, DNSProcessRequest.class),
                JsonSerde.of(jsonMapper, DNSResult.class));
    }

    @Override
    public void run(CommandLine cmd) {

    }

    @Override
    public @NotNull String getName() {
        return "dns-tls";
    }
}
