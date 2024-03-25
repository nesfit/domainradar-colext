package cz.vut.fit.domainradar.standalone.collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.vut.fit.domainradar.Topics;
import cz.vut.fit.domainradar.models.ResultCodes;
import cz.vut.fit.domainradar.models.results.ZoneResult;
import cz.vut.fit.domainradar.serialization.JsonSerde;
import cz.vut.fit.domainradar.standalone.BaseStandaloneCollector;
import cz.vut.fit.domainradar.standalone.IPStandaloneCollector;
import org.apache.commons.cli.CommandLine;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.xbill.DNS.ExtendedResolver;
import org.xbill.DNS.Name;
import org.xbill.DNS.TextParseException;
import org.xbill.DNS.Type;
import org.xbill.DNS.lookup.LookupSession;
import pl.tlinkowski.unij.api.UniLists;

import java.net.UnknownHostException;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ZoneCollector extends BaseStandaloneCollector<String, Void, String, ZoneResult> {
    private final ExecutorService _executor;
    private final InternalDNSResolver _dns;

    public ZoneCollector(@NotNull ObjectMapper jsonMapper,
                         @NotNull String appName,
                         @Nullable Properties properties) throws UnknownHostException {
        super(jsonMapper, appName, properties, Serdes.String(), Serdes.String(), Serdes.Void(),
                JsonSerde.of(jsonMapper, ZoneResult.class));

        // TODO: Configuration
        var dnsServers = new String[]{"195.113.144.194", "193.17.47.1", "195.113.144.233", "185.43.135.1"};

        var resolver = new ExtendedResolver(dnsServers);
        resolver.setTimeout(Duration.ofSeconds(10));
        resolver.setLoadBalance(false);
        resolver.setRetries(2);

        _executor = Executors.newVirtualThreadPerTaskExecutor();
        _dns = new InternalDNSResolver(resolver, _executor);
    }

    @Override
    public void run(CommandLine cmd) {
        buildProcessor(0);

        _parallelProcessor.subscribe(UniLists.of(Topics.IN_ZONE));
        _parallelProcessor.poll(ctx -> {
            var dn = ctx.key();
            _dns.getZoneInfo(dn)
                    .exceptionally(e -> {
                        // Shouldn't happen (error ought to be handled in the InternalDNSResolver)
                        return new ZoneResult(ResultCodes.OTHER_DNS_ERROR, e.getMessage(), Instant.now(), null);
                    }).thenAcceptAsync(result -> {
                        if (result == null)
                            // Shouldn't happen
                            result = new ZoneResult(ResultCodes.OTHER_DNS_ERROR,
                                    "Result null", Instant.now(), null);

                        System.err.println("producing result: " + result.toString());
                        _producer.send(new ProducerRecord<>(Topics.OUT_ZONE, dn, result));
                        // TODO: produce to IN_DNS
                    }, _executor);
        });
    }

    @Override
    public @NotNull String getName() {
        return "zone";
    }

    @Override
    public void close() {
        super.close();
        _executor.close();
    }
}
