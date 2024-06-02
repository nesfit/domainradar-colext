package cz.vut.fit.domainradar.standalone.collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.vut.fit.domainradar.Topics;
import cz.vut.fit.domainradar.models.IPToProcess;
import cz.vut.fit.domainradar.models.dns.DNSData;
import cz.vut.fit.domainradar.models.requests.DNSProcessRequest;
import cz.vut.fit.domainradar.models.results.DNSResult;
import cz.vut.fit.domainradar.serialization.JsonSerde;
import cz.vut.fit.domainradar.standalone.BaseStandaloneCollector;
import cz.vut.fit.domainradar.standalone.collectors.dns.Processor;
import io.vertx.core.*;
import io.vertx.core.dns.DnsClientOptions;
import io.vertx.core.dns.MxRecord;
import io.vertx.core.impl.VertxImpl;
import io.vertx.core.impl.VertxInternal;
import org.apache.commons.cli.CommandLine;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.VoidSerializer;
import org.jetbrains.annotations.NotNull;
import org.slf4j.LoggerFactory;
import pl.tlinkowski.unij.api.UniLists;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class VertxDNSCollector extends BaseStandaloneCollector<String, DNSProcessRequest> {
    private static final org.slf4j.Logger Logger = LoggerFactory.getLogger(VertxDNSCollector.class);

    public static String NAME = "dns";

    private final KafkaProducer<String, DNSResult> _resultProducer;
    private final KafkaProducer<IPToProcess, Void> _ipResultProducer;
    private final KafkaProducer<String, String> _tlsRequestProducer;

    private final Vertx _vertx;

    public VertxDNSCollector(@NotNull ObjectMapper jsonMapper,
                             @NotNull String appName,
                             @NotNull Properties properties) {
        super(jsonMapper, appName, properties,
                Serdes.String(),
                JsonSerde.of(jsonMapper, DNSProcessRequest.class));

        _resultProducer = super.createProducer(new StringSerializer(),
                JsonSerde.of(jsonMapper, DNSResult.class).serializer(), "result");
        _ipResultProducer = super.createProducer(JsonSerde.of(jsonMapper, IPToProcess.class).serializer(),
                new VoidSerializer(), "ip-req");
        _tlsRequestProducer = super.createProducer(new StringSerializer(), new StringSerializer(), "tls-req");

        _vertx = Vertx.vertx(new VertxOptions()
                .setWorkerPoolSize(32));
    }

    @Override
    public void run(CommandLine cmd) {
        // Batching would be possible here but I don't think it would be beneficial.
        // TODO: Examine this.
        buildProcessor(0);

        var dnsClient = new VerxDNSClient((VertxInternal) _vertx, new DnsClientOptions()
                .setHost("195.113.144.194")
                .setQueryTimeout(4000)
                .setLogActivity(false)
                .setRecursionDesired(true));

        _parallelProcessor.subscribe(UniLists.of(Topics.IN_DNS));
        _parallelProcessor.poll(ctx -> {
            final var dn = ctx.key();
            final var request = ctx.value();

            var aRes = dnsClient.resolveA(dn);
            //var aaaaRes = dnsClient.resolveAAAA(dn);
            var mxRes = dnsClient.resolveMX(dn);
            //var nsRes = dnsClient.resolveNS(dn);
            var txtRes = dnsClient.resolveTXT(dn);
            var cnameRes = dnsClient.resolveCNAME(dn);

            var allResults = Future.all(aRes, mxRes, txtRes, cnameRes);
            allResults.onComplete(this::onComplete, e -> {
                Logger.warn("Future error", e);
            });
        });
    }

    private void onComplete(AsyncResult<CompositeFuture> result) {
        if (result.failed()) {
            Logger.warn("Result failed", result.cause());
            return;
        }

        var composite = result.result();
        AsyncResult<List<String>> aRes = composite.resultAt(0);
        AsyncResult<List<MxRecord>> mxRes = composite.resultAt(1);
        AsyncResult<List<String>> txtRes = composite.resultAt(2);
        AsyncResult<List<String>> cnameRes = composite.resultAt(3);

        Logger.info("Ret");
    }

    @Override
    public @NotNull String getName() {
        return NAME;
    }

    @Override
    public void close() throws IOException {
        super.close();
        _resultProducer.close();
        _ipResultProducer.close();
        _tlsRequestProducer.close();
    }
}
