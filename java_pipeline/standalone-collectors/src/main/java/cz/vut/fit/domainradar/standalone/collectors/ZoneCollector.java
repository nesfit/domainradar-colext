package cz.vut.fit.domainradar.standalone.collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.vut.fit.domainradar.Topics;
import cz.vut.fit.domainradar.models.ResultCodes;
import cz.vut.fit.domainradar.models.requests.DNSProcessRequest;
import cz.vut.fit.domainradar.models.requests.RDAPDomainProcessRequest;
import cz.vut.fit.domainradar.models.requests.ZoneProcessRequest;
import cz.vut.fit.domainradar.models.results.ZoneResult;
import cz.vut.fit.domainradar.serialization.JsonSerde;
import cz.vut.fit.domainradar.standalone.TriProducerStandaloneCollector;
import org.apache.commons.cli.CommandLine;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import pl.tlinkowski.unij.api.UniLists;

import java.io.IOException;
import java.net.UnknownHostException;
import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ZoneCollector extends TriProducerStandaloneCollector<String, ZoneProcessRequest, String, ZoneResult,
        String, DNSProcessRequest, String, RDAPDomainProcessRequest> {
    public static final String NAME = "zone";
    private final ExecutorService _executor;
    private final InternalDNSResolver _dns;

    public ZoneCollector(@NotNull ObjectMapper jsonMapper,
                         @NotNull String appName,
                         @Nullable Properties properties) throws UnknownHostException {
        super(jsonMapper, appName, properties, Serdes.String(), Serdes.String(), Serdes.String(), Serdes.String(),
                JsonSerde.of(jsonMapper, ZoneProcessRequest.class),
                JsonSerde.of(jsonMapper, ZoneResult.class),
                JsonSerde.of(jsonMapper, DNSProcessRequest.class),
                JsonSerde.of(jsonMapper, RDAPDomainProcessRequest.class));

        _executor = Executors.newVirtualThreadPerTaskExecutor();
        _dns = new InternalDNSResolver(_executor, _properties);
    }

    @Override
    public void run(CommandLine cmd) {
        buildProcessor(0);
        final var defaultRequestValue = new ZoneProcessRequest(true, true, null, null);

        _parallelProcessor.subscribe(UniLists.of(Topics.IN_ZONE));
        _parallelProcessor.poll(ctx -> {
            var dn = ctx.key();
            _dns.getZoneInfo(dn)
                    .exceptionally(e -> {
                        // Shouldn't happen (error ought to be handled in the InternalDNSResolver)
                        return new ZoneResult(ResultCodes.OTHER_DNS_ERROR, e.getMessage(), Instant.now(), null);
                    }).thenAccept(result -> {
                        if (result == null)
                            // Shouldn't happen
                            result = new ZoneResult(ResultCodes.OTHER_DNS_ERROR,
                                    "Result null", Instant.now(), null);

                        _producer.send(new ProducerRecord<>(Topics.OUT_ZONE, dn, result));

                        if (result.zone() != null) {
                            var reqValue = ctx.value();
                            if (reqValue == null)
                                reqValue = defaultRequestValue;

                            if (reqValue.collectDNS()) {
                                _producer2.send(new ProducerRecord<>(Topics.IN_DNS, dn, new DNSProcessRequest(
                                        reqValue.dnsTypesToCollect(), reqValue.dnsTypesToProcessIPsFrom(), result.zone())));
                            }

                            if (reqValue.collectRDAP()) {
                                _producer3.send(new ProducerRecord<>(Topics.IN_RDAP_DN, dn, new RDAPDomainProcessRequest(
                                        result.zone().zone()
                                )));
                            }
                        } else {
                            _producer3.send(new ProducerRecord<>(Topics.IN_RDAP_DN, dn, new RDAPDomainProcessRequest(
                                    null)));
                        }
                    });
        });
    }

    @Override
    public @NotNull String getName() {
        return NAME;
    }

    @Override
    public void close() throws IOException {
        super.close();
        _executor.close();
    }
}
