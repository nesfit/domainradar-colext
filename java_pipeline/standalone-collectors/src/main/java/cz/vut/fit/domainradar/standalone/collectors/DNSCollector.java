package cz.vut.fit.domainradar.standalone.collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.vut.fit.domainradar.Topics;
import cz.vut.fit.domainradar.models.IPToProcess;
import cz.vut.fit.domainradar.models.requests.DNSRequest;
import cz.vut.fit.domainradar.models.results.DNSResult;
import cz.vut.fit.domainradar.serialization.JsonSerde;
import cz.vut.fit.domainradar.standalone.BaseStandaloneCollector;
import cz.vut.fit.domainradar.standalone.collectors.dns.Processor;
import org.apache.commons.cli.CommandLine;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.VoidSerializer;
import org.jetbrains.annotations.NotNull;
import pl.tlinkowski.unij.api.UniLists;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DNSCollector extends BaseStandaloneCollector<String, DNSRequest> {
    public static String NAME = "dns";

    private final ExecutorService _executor;
    private final Processor _fetchHandler;

    private final KafkaProducer<String, DNSResult> _resultProducer;
    private final KafkaProducer<IPToProcess, Void> _ipResultProducer;
    private final KafkaProducer<String, String> _tlsRequestProducer;

    public DNSCollector(@NotNull ObjectMapper jsonMapper,
                        @NotNull String appName,
                        @NotNull Properties properties) {
        super(jsonMapper, appName, properties,
                Serdes.String(),
                JsonSerde.of(jsonMapper, DNSRequest.class));

        _executor = Executors.newVirtualThreadPerTaskExecutor();

        _resultProducer = super.createProducer(new StringSerializer(),
                JsonSerde.of(jsonMapper, DNSResult.class).serializer(), "result");
        _ipResultProducer = super.createProducer(JsonSerde.of(jsonMapper, IPToProcess.class).serializer(),
                new VoidSerializer(), "ip-req");
        _tlsRequestProducer = super.createProducer(new StringSerializer(), new StringSerializer(), "tls-req");

        _fetchHandler = new Processor(properties, _resultProducer, _ipResultProducer, _tlsRequestProducer);
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
        _resultProducer.close();
        _ipResultProducer.close();
        _tlsRequestProducer.close();
    }
}
