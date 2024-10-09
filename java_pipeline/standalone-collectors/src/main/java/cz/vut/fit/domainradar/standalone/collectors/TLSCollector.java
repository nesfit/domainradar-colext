package cz.vut.fit.domainradar.standalone.collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.vut.fit.domainradar.CollectorConfig;
import cz.vut.fit.domainradar.Common;
import cz.vut.fit.domainradar.Topics;
import cz.vut.fit.domainradar.models.ResultCodes;
import cz.vut.fit.domainradar.models.results.TLSResult;
import cz.vut.fit.domainradar.models.tls.TLSData;
import cz.vut.fit.domainradar.serialization.JsonSerde;
import cz.vut.fit.domainradar.standalone.BaseStandaloneCollector;
import org.apache.commons.cli.CommandLine;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jetbrains.annotations.NotNull;
import pl.tlinkowski.unij.api.UniLists;

import javax.net.ssl.*;
import javax.security.auth.x500.X500Principal;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;

/**
 * A collector that processes TLS data for domain names.
 *
 * @author Ondřej Ondryáš
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

    public TLSCollector(@NotNull ObjectMapper jsonMapper, @NotNull String appName, @NotNull Properties properties) {
        super(jsonMapper, appName, properties,
                Serdes.String(), Serdes.String());
        _timeout = Integer.parseInt(properties.getProperty(CollectorConfig.TLS_TIMEOUT_MS_CONFIG,
                CollectorConfig.TLS_TIMEOUT_MS_DEFAULT));

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
        buildProcessor(0);
        // The timeout is used for connect and for the communication, so the absolute
        // processing bound must be slightly over its double
        final long futureTimeout = (long) (_timeout * 2.1);

        _parallelProcessor.subscribe(UniLists.of(Topics.IN_TLS));
        _parallelProcessor.poll(ctx -> {
            final var dn = ctx.key();
            final var ip = ctx.value();

            Logger.trace("Processing DN {} at {}", dn, ip);
            var resultFuture = runTLSResolve(dn, ip)
                    .orTimeout(futureTimeout, TimeUnit.MILLISECONDS);

            try {
                var result = resultFuture.join();
                _producer.send(resultRecord(Topics.OUT_TLS, dn, result));
            } catch (CompletionException e) {
                if (e.getCause() instanceof TimeoutException) {
                    Logger.debug("Operation timed out: {}", dn);
                    _producer.send(resultRecord(Topics.OUT_TLS, dn,
                            errorResult(ResultCodes.TIMEOUT, "Operation timed out (%d ms)".formatted(_timeout))));
                } else {
                    Logger.warn("Unexpected error: {}", dn, e);
                    _producer.send(resultRecord(Topics.OUT_TLS, dn,
                            errorResult(ResultCodes.INTERNAL_ERROR, e.getMessage())));
                }
            }
        });
    }

    public CompletableFuture<TLSResult> runTLSResolve(@NotNull String hostName, @NotNull String targetIp) {
        return CompletableFuture.supplyAsync(() -> {
            // Create a new SSL context with a naive trust manager that accepts all certificates
            SSLContext context;
            try {
                context = SSLContext.getInstance("TLS");
                context.init(null, new TrustManager[]{new NaiveTrustManager()}, null);
            } catch (NoSuchAlgorithmException | KeyManagementException e) {
                Logger.error("TLS context error", e);
                // Should not happen
                return errorResult(ResultCodes.INTERNAL_ERROR, e.getMessage());
            }

            try (var rawSocket = new Socket()) {
                try {
                    rawSocket.connect(new InetSocketAddress(targetIp, 443), _timeout);
                } catch (SocketTimeoutException e) {
                    Logger.debug("Connection timed out: {}", hostName);
                    return errorResult(ResultCodes.TIMEOUT, "Connection timed out (%d ms)".formatted(_timeout));
                } catch (IllegalArgumentException e) {
                    Logger.debug("Cannot use IPv6: {} at {}", hostName, targetIp);
                    return errorResult(ResultCodes.UNSUPPORTED_ADDRESS, "Cannot use this IP version");
                }

                // Make the TLS layer
                SSLSocketFactory factory = context.getSocketFactory();
                try (var socket = (SSLSocket) factory.createSocket(rawSocket, targetIp, 443, false)) {
                    // Enable timeouts
                    socket.setSoTimeout(_timeout);

                    // Enable SNI
                    SSLParameters sslParams = new SSLParameters();
                    sslParams.setServerNames(List.of(new SNIHostName(hostName)));
                    socket.setSSLParameters(sslParams);

                    // Start handshake to retrieve session details
                    Logger.trace("Starting TLS handshake: {}", hostName);
                    socket.startHandshake();

                    SSLSession session = socket.getSession();

                    // Extract negotiated protocol and cipher
                    var protocol = session.getProtocol();
                    var cipher = session.getCipherSuite();

                    // Extract certificates from the server
                    Certificate[] serverCerts = session.getPeerCertificates();
                    var certificates = new ArrayList<TLSData.Certificate>();

                    for (int i = 0, serverCertsLength = serverCerts.length; i < serverCertsLength; i++) {
                        var cert = serverCerts[i];
                        // Sometimes multiple references to the same certificate (object) are present in the array
                        if (i > 0 && cert == serverCerts[i - 1])
                            continue;

                        if (cert instanceof X509Certificate) {
                            certificates.add(parseCertificate((X509Certificate) cert));
                        }
                    }

                    final var tlsData = new TLSData(targetIp,
                            protocol, cipher, certificates);

                    Logger.trace("Success: {}", hostName);
                    return new TLSResult(ResultCodes.OK, null, Instant.now(), tlsData);
                } catch (SocketTimeoutException e) {
                    Logger.debug("Socket read timed out: {}", hostName);
                    return errorResult(ResultCodes.TIMEOUT, "Socket read timed out (%d ms)".formatted(_timeout));
                }
            } catch (IOException e) {
                Logger.debug("Cannot connect to {}: {}", hostName, e.getMessage());
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
        _producer.close(_closeTimeout);
        _executor.shutdown();
    }

    private static TLSResult errorResult(int code, String message) {
        return new TLSResult(code, message, Instant.now(), null);
    }
}
