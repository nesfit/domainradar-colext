package cz.vut.fit.domainradar.standalone.collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.vut.fit.domainradar.CollectorConfig;
import cz.vut.fit.domainradar.Topics;
import cz.vut.fit.domainradar.models.IPToProcess;
import cz.vut.fit.domainradar.models.ResultCodes;
import cz.vut.fit.domainradar.models.dns.DNSData;
import cz.vut.fit.domainradar.models.requests.DNSProcessRequest;
import cz.vut.fit.domainradar.models.results.DNSResult;
import cz.vut.fit.domainradar.models.tls.TLSData;
import cz.vut.fit.domainradar.serialization.JsonSerde;
import cz.vut.fit.domainradar.standalone.BiProducerStandaloneCollector;
import cz.vut.fit.domainradar.standalone.collectors.dns.RecordFetchHandler;
import org.apache.commons.cli.CommandLine;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.LoggerFactory;
import org.xbill.DNS.TextParseException;
import pl.tlinkowski.unij.api.UniLists;

import javax.net.ssl.*;
import javax.security.auth.x500.X500Principal;
import java.io.IOException;
import java.net.UnknownHostException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class NewDNSCollector extends BiProducerStandaloneCollector<String, DNSProcessRequest, String, DNSResult,
        IPToProcess, Void> {
    public static String NAME = "dns-tls";
    private static final org.slf4j.Logger Logger = LoggerFactory.getLogger(NewDNSCollector.class);

    private final ExecutorService _executor;
    private final RecordFetchHandler _fetchHandler;

    public NewDNSCollector(@NotNull ObjectMapper jsonMapper,
                           @NotNull String appName,
                           @NotNull Properties properties) {
        super(jsonMapper, appName, properties,
                Serdes.String(),
                Serdes.String(),
                JsonSerde.of(jsonMapper, IPToProcess.class),
                JsonSerde.of(jsonMapper, DNSProcessRequest.class),
                JsonSerde.of(jsonMapper, DNSResult.class),
                Serdes.Void());

        _executor = Executors.newVirtualThreadPerTaskExecutor();
        _fetchHandler = new RecordFetchHandler(properties, _producer, _producer2);
    }

    private List<String> parseConfig(String configKey, String defaultValue) {
        var config = _properties.getProperty(configKey, defaultValue);

        if (config.isEmpty())
            return null;

        return Arrays.asList(config.split(","));
    }

    @Override
    public void run(CommandLine cmd) {
        // Batching would be possible here but I don't think it would be beneficial.
        // TODO: Examine this.
        buildProcessor(0);

        _parallelProcessor.subscribe(UniLists.of(Topics.IN_DNS));
        _parallelProcessor.poll(ctx -> {
            final var dn = ctx.key();
            final var request = ctx.value();
            _fetchHandler.submit(dn, request);
        });
    }


    @Override
    public @NotNull String getName() {
        return NAME;
    }

    @Override
    public void close() throws IOException {
        super.close();
        _fetchHandler.close();
        _executor.close();
    }

    protected DNSResult errorResult(int code, @NotNull String message) {
        return new DNSResult(code, message, Instant.now(), null, null, null);
    }
}
