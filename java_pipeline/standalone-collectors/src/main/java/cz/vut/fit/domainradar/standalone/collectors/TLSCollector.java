package cz.vut.fit.domainradar.standalone.collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.vut.fit.domainradar.CollectorConfig;
import cz.vut.fit.domainradar.Common;
import cz.vut.fit.domainradar.Topics;
import cz.vut.fit.domainradar.models.ResultCodes;
import cz.vut.fit.domainradar.models.results.TLSResult;
import cz.vut.fit.domainradar.serialization.JsonSerde;
import cz.vut.fit.domainradar.standalone.BaseStandaloneCollector;
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
 * @see TLSCollectorImpl
 */
public class TLSCollector extends BaseStandaloneCollector<String, String> {
    public static final String NAME = "tls";
    public static final String COMPONENT_NAME = "collector-" + NAME;
    private static final org.slf4j.Logger Logger = Common.getComponentLogger(TLSCollector.class);


    /**
     * A naive trust manager that accepts all certificates.
     */
    static class NaiveTrustManager implements X509TrustManager {
        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType) {
            // Never throw -> accept all certificates
        }

        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType) {
            // Not used
        }
    }

    private final KafkaProducer<String, TLSResult> _producer;
    private final ExecutorService _executor;
    private final int _timeout;
    private final TLSCollectorImpl _collector;

    public TLSCollector(@NotNull ObjectMapper jsonMapper, @NotNull String appName, @NotNull Properties properties) {
        super(jsonMapper, appName, properties,
                Serdes.String(), Serdes.String());
        _timeout = Integer.parseInt(properties.getProperty(CollectorConfig.TLS_TIMEOUT_MS_CONFIG,
                CollectorConfig.TLS_TIMEOUT_MS_DEFAULT));
        var maxRedirects = Integer.parseInt(properties.getProperty(CollectorConfig.TLS_TIMEOUT_MS_CONFIG,
                CollectorConfig.TLS_TIMEOUT_MS_DEFAULT));

        _collector = new TLSCollectorImpl(maxRedirects, _timeout);
        _executor = Executors.newVirtualThreadPerTaskExecutor();
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
                _producer.send(resultRecord(Topics.OUT_TLS, dn, result));
            } catch (CompletionException e) {
                if (e.getCause() instanceof TimeoutException) {
                    Logger.debug("Operation timed out: {}", dn);
                    _producer.send(resultRecord(Topics.OUT_TLS, dn,
                            TLSCollectorImpl.errorResult(ResultCodes.TIMEOUT, "Operation timed out (%d ms)".formatted(_timeout))));
                } else {
                    Logger.warn("Unexpected error: {}", dn, e);
                    _producer.send(resultRecord(Topics.OUT_TLS, dn,
                            TLSCollectorImpl.errorResult(ResultCodes.INTERNAL_ERROR, e.getMessage())));
                }
            }
        });
    }

    @Override
    public @NotNull String getName() {
        return NAME;
    }

    @Override
    public void close() {
        _producer.close(_closeTimeout);
        _executor.shutdown();
    }
}
