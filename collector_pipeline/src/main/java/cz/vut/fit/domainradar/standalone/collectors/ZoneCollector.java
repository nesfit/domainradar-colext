package cz.vut.fit.domainradar.standalone.collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.vut.fit.domainradar.models.results.ZoneResult;
import cz.vut.fit.domainradar.serialization.JsonSerde;
import cz.vut.fit.domainradar.standalone.BaseStandaloneCollector;
import org.apache.commons.cli.CommandLine;
import org.apache.kafka.common.serialization.Serdes;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.xbill.DNS.ExtendedResolver;

import java.net.UnknownHostException;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ZoneCollector extends BaseStandaloneCollector<String, Void, String, ZoneResult> {
    private final ExtendedResolver _mainResolver;
    private final ExecutorService _executor;
    private final InternalDNSResolver _dns;

    public ZoneCollector(@NotNull ObjectMapper jsonMapper,
                         @NotNull String appName,
                         @Nullable Properties properties) throws UnknownHostException {
        super(jsonMapper, appName, properties, Serdes.String(), Serdes.String(), Serdes.Void(),
                JsonSerde.of(jsonMapper, ZoneResult.class));

        // TODO: Configuration
        var dnsServers = new String[]{"45.46.176.240"};//{"195.113.144.194", "193.17.47.1", "195.113.144.233", "185.43.135.1"};

        var resolver = new ExtendedResolver(dnsServers);
        resolver.setTimeout(Duration.ofSeconds(4));
        resolver.setLoadBalance(false);
        resolver.setRetries(2);

        _mainResolver = resolver;
        _executor = Executors.newVirtualThreadPerTaskExecutor();
        _dns = new InternalDNSResolver(resolver, _executor);
    }

    @Override
    public void run(CommandLine cmd) {
        var x = _dns.resolveIps("seznam.cz");
    }

    @Override
    public @NotNull String getName() {
        return "zone";
    }

    @Override
    public void close() {
        _executor.close();
        super.close();
    }
}
