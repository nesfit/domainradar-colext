package cz.vut.fit.domainradar.standalone.collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.vut.fit.domainradar.CollectorConfig;
import cz.vut.fit.domainradar.Common;
import cz.vut.fit.domainradar.Topics;
import cz.vut.fit.domainradar.models.ResultCodes;
import cz.vut.fit.domainradar.models.results.TLSResult;
import cz.vut.fit.domainradar.serialization.JsonSerde;
import cz.vut.fit.domainradar.standalone.BaseStandaloneCollector;
import cz.vut.fit.domainradar.standalone.https.HTTPSFetcherBase;
import cz.vut.fit.domainradar.standalone.https.HTTPSFetcherImpl;
import io.confluent.parallelconsumer.ParallelStreamProcessor;
import org.apache.commons.cli.CommandLine;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jetbrains.annotations.NotNull;
import pl.tlinkowski.unij.api.UniLists;

import javax.net.ssl.*;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.*;


/**
 * A collector that processes TLS data for domain names.
 *
 * @author Ondřej Ondryáš
 * @see cz.vut.fit.domainradar.standalone.https.HTTPSFetcherBase
 */
public class TLSCollector
        extends BaseStandaloneCollector<String, String, ParallelStreamProcessor<String, String>> {
    public static final String NAME = "tls";
    public static final String COMPONENT_NAME = "collector-" + NAME;
    private static final org.slf4j.Logger Logger = Common.getComponentLogger(TLSCollector.class);


    private final KafkaProducer<String, TLSResult> _producer;
    private final ExecutorService _executor;
    private final int _timeout;
    private final boolean _tryHTTPOnly;
    private final HTTPSFetcherBase _collector;

    public TLSCollector(@NotNull ObjectMapper jsonMapper, @NotNull String appName, @NotNull Properties properties) {
        super(jsonMapper, appName, properties,
                Serdes.String(), Serdes.String());
        _timeout = Integer.parseInt(properties.getProperty(CollectorConfig.TLS_TIMEOUT_MS_CONFIG,
                CollectorConfig.TLS_TIMEOUT_MS_DEFAULT));
        _tryHTTPOnly = Boolean.parseBoolean(properties.getProperty(CollectorConfig.TLS_TRY_HTTP_ONLY_CONFIG,
                CollectorConfig.TLS_TRY_HTTP_ONLY_DEFAULT));
        var maxRedirects = Integer.parseInt(properties.getProperty(CollectorConfig.TLS_MAX_REDIRECTS_CONFIG,
                CollectorConfig.TLS_MAX_REDIRECTS_DEFAULT));

        _executor = Executors.newVirtualThreadPerTaskExecutor();
        _collector = new HTTPSFetcherImpl(maxRedirects, _timeout, _executor, Logger);
        _producer = super.createProducer(new StringSerializer(),
                JsonSerde.of(jsonMapper, TLSResult.class).serializer());

        try {
            // Determine the runtime SSL/TLS capabilities
            var sslEngine = SSLContext.getDefault().createSSLEngine();
            var enabledProtocols = Arrays.toString(sslEngine.getEnabledProtocols());
            var enabledCiphers = Arrays.toString(sslEngine.getEnabledCipherSuites());
            Logger.info("TLS enabled protocols: {}", enabledProtocols);
            Logger.info("TLS enabled ciphers: {}", enabledCiphers);
        } catch (NoSuchAlgorithmException e) {
            Logger.error("Cannot get the default SSL context", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void run(CommandLine cmd) {
        // The timeout is used for connect and for the communication, so the absolute
        // processing bound must be slightly over its double
        final long futureTimeout = (long) (_timeout * 2.1);
        buildProcessor(0, futureTimeout);

        _parallelProcessor.subscribe(UniLists.of(Topics.IN_TLS));
        _parallelProcessor.poll(ctx -> {
            final var dn = ctx.key();
            final var ip = ctx.value();

            Logger.trace("Processing DN {} at {}", dn, ip);
            var resultFuture = CompletableFuture.supplyAsync(() -> _collector.collect(dn, ip), _executor)
                    .orTimeout(futureTimeout, TimeUnit.MILLISECONDS);

            try {
                var result = resultFuture.join();
                if (_tryHTTPOnly && result.statusCode() != 0 && result.html() == null) {
                    // Try HTTP to fetch the HTML
                    var httpFuture = _collector.collectHTTPOnly(dn, ip);
                    try {
                        var html = httpFuture.orTimeout(futureTimeout, TimeUnit.MILLISECONDS).join();
                        result = new TLSResult(result.statusCode(), result.error(),
                                result.lastAttempt(), result.tlsData(), html);
                    } catch (CompletionException e) {
                        // intentionally ignore
                    }
                }

                _producer.send(resultRecord(Topics.OUT_TLS, dn, result));
            } catch (CompletionException e) {
                if (e.getCause() instanceof TimeoutException) {
                    Logger.debug("Operation timed out: {}", dn);
                    _producer.send(resultRecord(Topics.OUT_TLS, dn,
                            HTTPSFetcherBase.errorResult(ResultCodes.TIMEOUT, "Operation timed out (%d ms)".formatted(_timeout))));
                } else {
                    Logger.warn("Unexpected error: {}", dn, e.getCause());
                    _producer.send(resultRecord(Topics.OUT_TLS, dn,
                            HTTPSFetcherBase.errorResult(ResultCodes.INTERNAL_ERROR, e.getMessage())));
                }
            }
        });
    }

    @Override
    public @NotNull String getName() {
        return NAME;
    }

    protected void buildProcessor(int batchSize, long timeoutMs) {
        _parallelProcessor = ParallelStreamProcessor.createEosStreamProcessor(
                this.buildProcessorOptions(batchSize, timeoutMs)
        );
    }

    @Override
    public void close() {
        _producer.close(_closeTimeout);
        _executor.shutdown();
    }
}
