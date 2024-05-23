package cz.vut.fit.domainradar.standalone.collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.vut.fit.domainradar.CollectorConfig;
import cz.vut.fit.domainradar.Topics;
import cz.vut.fit.domainradar.models.ResultCodes;
import cz.vut.fit.domainradar.models.results.TLSResult;
import cz.vut.fit.domainradar.models.tls.TLSData;
import cz.vut.fit.domainradar.serialization.JsonSerde;
import cz.vut.fit.domainradar.standalone.BaseStandaloneCollector;
import org.apache.commons.cli.CommandLine;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jetbrains.annotations.NotNull;
import pl.tlinkowski.unij.api.UniLists;

import javax.net.ssl.*;
import javax.security.auth.x500.X500Principal;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;

public class TLSCollector extends BaseStandaloneCollector<String, String> {
    public static final String NAME = "tls";

    private final KafkaProducer<String, TLSResult> _producer;
    private final ExecutorService _executor;
    private final long _timeout;

    public TLSCollector(@NotNull ObjectMapper jsonMapper, @NotNull String appName, @NotNull Properties properties) {
        super(jsonMapper, appName, properties,
                Serdes.String(), Serdes.String());
        _timeout = Long.parseLong(properties.getProperty(CollectorConfig.TLS_TIMEOUT_MS_CONFIG,
                CollectorConfig.TLS_TIMEOUT_MS_DEFAULT));

        _executor = Executors.newVirtualThreadPerTaskExecutor();
        _producer = super.createProducer(new StringSerializer(),
                JsonSerde.of(jsonMapper, TLSResult.class).serializer());
    }

    @Override
    public void run(CommandLine cmd) {
        buildProcessor(0);
        final long futureTimeout = (long) (_timeout * 1.1);

        _parallelProcessor.subscribe(UniLists.of(Topics.IN_TLS));
        _parallelProcessor.poll(ctx -> {
            final var dn = ctx.key();
            final var ip = ctx.value();

            var resultFuture = runTLSResolve(dn, ip).toCompletableFuture()
                    .orTimeout(futureTimeout, TimeUnit.MILLISECONDS);

            try {
                var result = resultFuture.join();
                _producer.send(new ProducerRecord<>(Topics.OUT_TLS, dn, result));
            } catch (CompletionException e) {
                if (e.getCause() instanceof TimeoutException) {
                    _producer.send(new ProducerRecord<>(Topics.OUT_TLS, dn,
                            errorResult(ResultCodes.CANNOT_FETCH, "TLS timeout")));
                } else {
                    _producer.send(new ProducerRecord<>(Topics.OUT_TLS, dn,
                            errorResult(ResultCodes.INTERNAL_ERROR, e.getMessage())));
                }
            }
        });
    }

    public CompletableFuture<TLSResult> runTLSResolve(@NotNull String hostName, @NotNull String targetIp) {
        return CompletableFuture.supplyAsync(() -> {
            SSLContext context;
            try {
                context = SSLContext.getInstance("TLS");
                context.init(null, null, null);
            } catch (NoSuchAlgorithmException | KeyManagementException e) {
                // Should not happen
                return errorResult(ResultCodes.INTERNAL_ERROR, e.getMessage());
            }

            SSLSocketFactory factory = context.getSocketFactory();

            try (SSLSocket socket = (SSLSocket) factory.createSocket(targetIp, 443)) {
                // Enable timeouts
                socket.setSoTimeout((int) _timeout);

                // Enable SNI
                SSLParameters sslParams = new SSLParameters();
                sslParams.setServerNames(List.of(new SNIHostName(hostName)));
                socket.setSSLParameters(sslParams);

                // Start handshake to retrieve session details
                socket.startHandshake();

                SSLSession session = socket.getSession();

                // Extract negotiated protocol and cipher
                var protocol = session.getProtocol();
                var cipher = session.getCipherSuite();

                // Extract certificates from the server
                Certificate[] serverCerts = session.getPeerCertificates();
                var certificates = new ArrayList<TLSData.Certificate>();
                for (var cert : serverCerts) {
                    if (cert instanceof X509Certificate) {
                        certificates.add(parseCertificate((X509Certificate) cert));
                    }
                }

                final var tlsData = new TLSData(targetIp,
                        protocol, cipher, certificates);

                return new TLSResult(ResultCodes.OK, null, Instant.now(), tlsData);
            } catch (IOException e) {
                return errorResult(ResultCodes.CANNOT_FETCH, e.getMessage());
            }
        }, _executor);
    }

    public static TLSData.Certificate parseCertificate(X509Certificate cert) {
        X500Principal subject = cert.getSubjectX500Principal();
        String subjectDN = subject.getName(X500Principal.RFC1779);

        try {
            return new TLSData.Certificate(subjectDN, cert.getEncoded());
        } catch (CertificateEncodingException e) {
            return new TLSData.Certificate(subjectDN, new byte[0]);
        }
    }

    @Override
    public @NotNull String getName() {
        return NAME;
    }

    @Override
    public void close() {
        _producer.close();
        _executor.shutdown();
    }

    private static TLSResult errorResult(int code, String message) {
        return new TLSResult(code, message, Instant.now(), null);
    }
}
