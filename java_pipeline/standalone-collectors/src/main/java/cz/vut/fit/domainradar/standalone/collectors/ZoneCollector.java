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
import org.slf4j.LoggerFactory;
import pl.tlinkowski.unij.api.UniLists;

import java.io.IOException;
import java.net.UnknownHostException;
import java.time.Instant;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.*;

/**
 * @deprecated The Java implementation of the Zone collector is currently not really working reliably – for some reason,
 * when large amounts of requests are to be handled simultaneously, the collector gets congested and soon begins to
 * end the requests with timeouts. For the moment, the Python implementation is used instead. It may not offer as much
 * true parallelization, but it works and seems to suffice for the current needs.
 */
@Deprecated()
public class ZoneCollector extends BaseStandaloneCollector<String, ZoneProcessRequest> {
    public static final String NAME = "zone";
    private static final org.slf4j.Logger Logger = LoggerFactory.getLogger(ZoneCollector.class);

    private final KafkaProducer<String, ZoneResult> _resultProducer;
    private final KafkaProducer<String, DNSProcessRequest> _dnsRequestProducer;
    private final KafkaProducer<String, RDAPDomainProcessRequest> _rdapRequestProducer;

    private final ExecutorService _executor;
    private final InternalDNSResolver _dns;

    private final long _zoneResolutionTimeout;

    public ZoneCollector(@NotNull ObjectMapper jsonMapper,
                         @NotNull String appName,
                         @NotNull Properties properties) throws UnknownHostException {
        super(jsonMapper, appName, properties, Serdes.String(), JsonSerde.of(jsonMapper, ZoneProcessRequest.class));

        _zoneResolutionTimeout = Long.parseLong(properties.getProperty(CollectorConfig.ZONE_RESOLUTION_TIMEOUT_MS_CONFIG,
                CollectorConfig.ZONE_RESOLUTION_TIMEOUT_MS_DEFAULT));

        _resultProducer = super.createProducer(new StringSerializer(),
                JsonSerde.of(jsonMapper, ZoneResult.class).serializer(), "result");
        _dnsRequestProducer = super.createProducer(new StringSerializer(),
                JsonSerde.of(jsonMapper, DNSProcessRequest.class).serializer(), "dns-req");
        _rdapRequestProducer = super.createProducer(new StringSerializer(),
                JsonSerde.of(jsonMapper, RDAPDomainProcessRequest.class).serializer(), "rdap-req");

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

            Logger.trace("Processing request for {}", dn);
            var requestFuture = processRequest(dn, ctx.value(), defaultRequestValue)
                    .orTimeout(_zoneResolutionTimeout, TimeUnit.MILLISECONDS);

            try {
                requestFuture.join();
            } catch (CompletionException e) {
                if (e.getCause() instanceof TimeoutException) {
                    Logger.debug("Timeout: {}", dn);
                    _resultProducer.send(new ProducerRecord<>(Topics.OUT_ZONE, dn,
                            new ZoneResult(ResultCodes.CANNOT_FETCH, "Zone resolution timeout",
                                    Instant.now(), null)));
                } else {
                    Logger.debug("Error for {}", dn, e.getCause());
                    _resultProducer.send(new ProducerRecord<>(Topics.OUT_ZONE, dn,
                            new ZoneResult(ResultCodes.INTERNAL_ERROR, e.getCause().getMessage(),
                                    Instant.now(), null)));
                }
            }
        });
    }

    private CompletableFuture<Void> processRequest(final String dn, final ZoneProcessRequest reqValue,
                                                   final ZoneProcessRequest defaultRequestValue) {
        return _dns.getZoneInfo(dn)
                .exceptionally(e -> {
                    // Shouldn't happen (error ought to be handled in the InternalDNSResolver)
                    Logger.warn("Unexpected exceptional completion of {}", dn, e);
                    return new ZoneResult(ResultCodes.OTHER_DNS_ERROR, e.getMessage(), Instant.now(), null);
                }).thenAccept(result -> {
                    if (result == null) { // Shouldn't happen
                        Logger.warn("Unexpected null response of {}", dn);
                        result = new ZoneResult(ResultCodes.OTHER_DNS_ERROR,
                                "Result null", Instant.now(), null);
                    }

                    Logger.trace("Processed {}", dn);
                    _resultProducer.send(new ProducerRecord<>(Topics.OUT_ZONE, dn, result));

                    if (result.zone() != null) {
                        var request = Objects.requireNonNullElse(reqValue, defaultRequestValue);

                        if (request.collectDNS()) {
                            _dnsRequestProducer.send(new ProducerRecord<>(Topics.IN_DNS, dn, new DNSProcessRequest(
                                    request.dnsTypesToCollect(), request.dnsTypesToProcessIPsFrom(), result.zone())));
                        }

                        if (request.collectRDAP()) {
                            _rdapRequestProducer.send(new ProducerRecord<>(Topics.IN_RDAP_DN, dn,
                                    new RDAPDomainProcessRequest(result.zone().zone())));
                        }
                    } else {
                        _rdapRequestProducer.send(new ProducerRecord<>(Topics.IN_RDAP_DN, dn,
                                new RDAPDomainProcessRequest(null)));
                    }
                }).toCompletableFuture();
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
