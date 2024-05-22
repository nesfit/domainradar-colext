package cz.vut.fit.domainradar.standalone.collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.vut.fit.domainradar.CollectorConfig;
import cz.vut.fit.domainradar.Topics;
import cz.vut.fit.domainradar.models.ResultCodes;
import cz.vut.fit.domainradar.models.requests.DNSProcessRequest;
import cz.vut.fit.domainradar.models.requests.RDAPDomainProcessRequest;
import cz.vut.fit.domainradar.models.requests.ZoneProcessRequest;
import cz.vut.fit.domainradar.models.results.ZoneResult;
import cz.vut.fit.domainradar.serialization.JsonSerde;
import cz.vut.fit.domainradar.standalone.BaseStandaloneCollector;
import org.apache.commons.cli.CommandLine;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import pl.tlinkowski.unij.api.UniLists;

import java.io.IOException;
import java.net.UnknownHostException;
import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.*;

public class ZoneCollector extends BaseStandaloneCollector<String, ZoneProcessRequest> {
    public static final String NAME = "zone";
    private final ExecutorService _executor;
    private final InternalDNSResolver _dns;

    private final KafkaProducer<String, ZoneResult> _resultProducer;
    private final KafkaProducer<String, DNSProcessRequest> _dnsRequestProducer;
    private final KafkaProducer<String, RDAPDomainProcessRequest> _rdapRequestProducer;

    private final long _zoneResolutionTimeout;

    public ZoneCollector(@NotNull ObjectMapper jsonMapper,
                         @NotNull String appName,
                         @NotNull Properties properties) throws UnknownHostException {
        super(jsonMapper, appName, properties, Serdes.String(), JsonSerde.of(jsonMapper, ZoneProcessRequest.class));

        _zoneResolutionTimeout = Long.parseLong(properties.getProperty(CollectorConfig.ZONE_RESOLUTION_TIMEOUT_MS_CONFIG,
                CollectorConfig.ZONE_RESOLUTION_TIMEOUT_MS_DEFAULT));

        _resultProducer = super.createProducer(new StringSerializer(),
                JsonSerde.of(jsonMapper, ZoneResult.class).serializer());
        _dnsRequestProducer = super.createProducer(new StringSerializer(),
                JsonSerde.of(jsonMapper, DNSProcessRequest.class).serializer());
        _rdapRequestProducer = super.createProducer(new StringSerializer(),
                JsonSerde.of(jsonMapper, RDAPDomainProcessRequest.class).serializer());

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

            var requestFuture = _dns.getZoneInfo(dn)
                    .exceptionally(e -> {
                        // Shouldn't happen (error ought to be handled in the InternalDNSResolver)
                        return new ZoneResult(ResultCodes.OTHER_DNS_ERROR, e.getMessage(), Instant.now(), null);
                    }).thenAccept(result -> {
                        if (result == null)
                            // Shouldn't happen
                            result = new ZoneResult(ResultCodes.OTHER_DNS_ERROR,
                                    "Result null", Instant.now(), null);

                        _resultProducer.send(new ProducerRecord<>(Topics.OUT_ZONE, dn, result));

                        if (result.zone() != null) {
                            var reqValue = ctx.value();
                            if (reqValue == null)
                                reqValue = defaultRequestValue;

                            if (reqValue.collectDNS()) {
                                _dnsRequestProducer.send(new ProducerRecord<>(Topics.IN_DNS, dn, new DNSProcessRequest(
                                        reqValue.dnsTypesToCollect(), reqValue.dnsTypesToProcessIPsFrom(), result.zone())));
                            }

                            if (reqValue.collectRDAP()) {
                                _rdapRequestProducer.send(new ProducerRecord<>(Topics.IN_RDAP_DN, dn,
                                        new RDAPDomainProcessRequest(result.zone().zone())));
                            }
                        } else {
                            _rdapRequestProducer.send(new ProducerRecord<>(Topics.IN_RDAP_DN, dn,
                                    new RDAPDomainProcessRequest(null)));
                        }
                    }).toCompletableFuture();

            try {
                requestFuture.orTimeout(_zoneResolutionTimeout, TimeUnit.MILLISECONDS).join();
            } catch (CompletionException e) {
                if (e.getCause() instanceof TimeoutException) {
                    _resultProducer.send(new ProducerRecord<>(Topics.OUT_ZONE, dn,
                            new ZoneResult(ResultCodes.CANNOT_FETCH, "Zone resolution timeout",
                                    Instant.now(), null)));
                } else {
                    _resultProducer.send(new ProducerRecord<>(Topics.OUT_ZONE, dn,
                            new ZoneResult(ResultCodes.INTERNAL_ERROR, e.getCause().getMessage(),
                                    Instant.now(), null)));
                }
            }
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
