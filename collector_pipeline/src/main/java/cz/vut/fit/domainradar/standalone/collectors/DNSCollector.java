package cz.vut.fit.domainradar.standalone.collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.vut.fit.domainradar.models.requests.DNSProcessRequest;
import cz.vut.fit.domainradar.models.results.DNSResult;
import cz.vut.fit.domainradar.serialization.JsonSerde;
import cz.vut.fit.domainradar.standalone.BaseStandaloneCollector;
import cz.vut.fit.domainradar.standalone.StandaloneCollectorRunner;
import org.apache.commons.cli.CommandLine;
import org.apache.kafka.common.serialization.Serdes;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xbill.DNS.*;
import org.xbill.DNS.lookup.LookupSession;

import java.lang.Record;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DNSCollector extends BaseStandaloneCollector<String, DNSProcessRequest, String, DNSResult> {
    private static final org.slf4j.Logger Logger = LoggerFactory.getLogger(DNSCollector.class);

    private final Resolver _mainResolver;
    private final ExecutorService _executor;
    private final InternalDNSResolver _dns;

    public DNSCollector(@NotNull ObjectMapper jsonMapper,
                        @NotNull String appName,
                        @Nullable Properties properties) throws UnknownHostException {
        super(jsonMapper, appName, properties,
                Serdes.String(),
                Serdes.String(),
                JsonSerde.of(jsonMapper, DNSProcessRequest.class),
                JsonSerde.of(jsonMapper, DNSResult.class));

        // TODO: Configuration
        var dnsServers = new String[]{"195.113.144.194", "193.17.47.1", "195.113.144.233", "185.43.135.1"};

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
        buildProcessor(0);
    }

    @Override
    public @NotNull String getName() {
        return "dns-tls";
    }

    @Override
    public void close() {
        try {
            _executor.awaitTermination(5, TimeUnit.SECONDS); // TODO
        } catch (InterruptedException e) {
            // Ignored
        }

        super.close();
        _executor.close();
    }
}
