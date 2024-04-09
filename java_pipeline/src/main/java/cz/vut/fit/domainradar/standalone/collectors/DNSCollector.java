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
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DNSCollector extends BiProducerStandaloneCollector<String, DNSProcessRequest, String, DNSResult,
        IPToProcess, Void> {
    private static final org.slf4j.Logger Logger = LoggerFactory.getLogger(DNSCollector.class);

    private final ExecutorService _executor;
    private final InternalDNSResolver _dns;

    private final List<String> _toCollect, _typesToProcessIPsFrom;

    public DNSCollector(@NotNull ObjectMapper jsonMapper,
                        @NotNull String appName,
                        @Nullable Properties properties) throws UnknownHostException {
        super(jsonMapper, appName, properties,
                Serdes.String(),
                Serdes.String(),
                JsonSerde.of(jsonMapper, IPToProcess.class),
                JsonSerde.of(jsonMapper, DNSProcessRequest.class),
                JsonSerde.of(jsonMapper, DNSResult.class),
                Serdes.Void());

        _executor = Executors.newVirtualThreadPerTaskExecutor();
        _dns = new InternalDNSResolver(_executor, _properties);

        _toCollect = this.parseConfig(CollectorConfig.DNS_DEFAULT_RECORD_TYPES_TO_COLLECT_CONFIG,
                CollectorConfig.DNS_DEFAULT_RECORD_TYPES_TO_COLLECT_DEFAULT);
        _typesToProcessIPsFrom = this.parseConfig(CollectorConfig.DNS_DEFAULT_TYPES_TO_COLLECT_IPS_FROM_CONFIG,
                CollectorConfig.DNS_DEFAULT_TYPES_TO_COLLECT_IPS_FROM_DEFAULT);
    }

    private List<String> parseConfig(String configKey, String defaultValue) {
        var config = _properties.getProperty(configKey, defaultValue);

        if (config.isEmpty())
            return null;

        return Arrays.asList(config.split(","));
    }

    @Override
    public void run(CommandLine cmd) {
        buildProcessor(0);

        _parallelProcessor.subscribe(UniLists.of(Topics.IN_DNS));
        _parallelProcessor.poll(ctx -> {
            final var dn = ctx.key();
            final var request = ctx.value();

            InternalDNSResolver.DNSScanner scanner;
            try {
                scanner = _dns.makeScanner(dn, request.zoneInfo());
            } catch (TextParseException e) {
                send(Topics.OUT_DNS, dn, errorResult(ResultCodes.INVALID_DOMAIN_NAME, e.getMessage()));
                return;
            }

            scanner.scan(_toCollect)
                    .handle((result, exc) -> {
                        if (exc != null) {
                            return CompletableFuture.completedFuture(errorResult(ResultCodes.OTHER_DNS_ERROR,
                                    exc.getMessage()));
                        } else {
                            return runTlsResolve(dn, result, _executor);
                        }
                    })
                    .thenCompose(Function.identity())
                    .thenAccept(result -> {
                        if (result == null)
                            return;

                        send(Topics.OUT_DNS, dn, result);
                        if (result.ips() != null) {
                            var unique = result.ips().stream().map(DNSResult.IPFromRecord::ip)
                                    .collect(Collectors.toSet());

                            // TODO: Use transactions? https://www.confluent.io/blog/transactions-apache-kafka/
                            for (var ip : unique) {
                                _producer2.send(new ProducerRecord<>(Topics.IN_IP,
                                        new IPToProcess(dn, ip), null));
                            }
                        }
                    });
        });
    }

    public CompletableFuture<DNSResult> runTlsResolve(String hostName, DNSData dnsData, ExecutorService executor) {
        String targetIp, fromType;
        // TODO: handle empty sets
        if (dnsData.CNAME() != null && dnsData.CNAME().relatedIps() != null && !dnsData.CNAME().relatedIps().isEmpty()) {
            targetIp = dnsData.CNAME().relatedIps().getFirst();
            fromType = "CNAME";
        } else if (dnsData.A() != null) {
            targetIp = dnsData.A().iterator().next();
            fromType = "A";
        } else if (dnsData.AAAA() != null) {
            targetIp = dnsData.AAAA().iterator().next();
            fromType = "AAAA";
        } else {
            targetIp = fromType = null;
        }

        if (targetIp == null) {
            // TODO: log
            return CompletableFuture.completedFuture(new DNSResult(ResultCodes.OK, null, Instant.now(),
                    dnsData, null, makeIps(dnsData)));
        }

        return CompletableFuture.supplyAsync(() -> {
            SSLContext context;
            try {
                context = SSLContext.getInstance("TLS");
                context.init(null, null, null);
            } catch (NoSuchAlgorithmException | KeyManagementException e) {
                // Should not happen
                // TODO: log
                return new DNSResult(ResultCodes.OK, null, Instant.now(),
                        dnsData, null, makeIps(dnsData));
            }

            SSLSocketFactory factory = context.getSocketFactory();

            try (SSLSocket socket = (SSLSocket) factory.createSocket(targetIp, 443)) {
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

                final var tlsData = new TLSData(new DNSResult.IPFromRecord(targetIp, fromType),
                        protocol, cipher, certificates);
                return new DNSResult(ResultCodes.OK, null, Instant.now(),
                        dnsData, tlsData, makeIps(dnsData));
            } catch (IOException e) {
                // TODO: log
                return new DNSResult(ResultCodes.OK, null, Instant.now(),
                        dnsData, null, makeIps(dnsData));
            }
        }, executor);
    }

    public static TLSData.Certificate parseCertificate(X509Certificate cert) {
        String commonName = "", country = "", organization = "";
        boolean isRoot = cert.getBasicConstraints() != -1; // A simple check for root CA
        Instant validityStart = cert.getNotBefore().toInstant();
        Instant validityEnd = cert.getNotAfter().toInstant();
        long validLen = validityEnd.getEpochSecond() - validityStart.getEpochSecond();

        // Parsing the subject DN for CN, O, and C
        X500Principal subject = cert.getSubjectX500Principal();
        String subjectDN = subject.getName(X500Principal.RFC1779);
        String[] subjectParts = subjectDN.split(",");
        for (String part : subjectParts) {
            part = part.trim();
            if (part.startsWith("CN=")) {
                commonName = part.substring(3);
            } else if (part.startsWith("O=")) {
                organization = part.substring(2);
            } else if (part.startsWith("C=")) {
                country = part.substring(2);
            }
        }

        // Extracting extensions
        List<TLSData.CertificateExtension> extensions = new ArrayList<>();
        var encoder = Base64.getEncoder();
        for (var extensionOID : cert.getCriticalExtensionOIDs()) {
            extensions.add(new TLSData.CertificateExtension(true, extensionOID, encoder.encodeToString(
                    cert.getExtensionValue(extensionOID))));
        }

        for (var extensionOID : cert.getNonCriticalExtensionOIDs()) {
            extensions.add(new TLSData.CertificateExtension(false, extensionOID, encoder.encodeToString(
                    cert.getExtensionValue(extensionOID))));
        }

        return new TLSData.Certificate(commonName, country, isRoot, organization,
                (int) validLen, validityEnd, validityStart, extensions.size(), extensions);
    }

    private static <T> Stream<T> streamIfNotNull(Collection<T> collection) {
        return collection == null ? Stream.empty() : collection.stream();
    }

    private Set<DNSResult.IPFromRecord> makeIps(DNSData data) {
        if (_typesToProcessIPsFrom == null)
            return null;

        var ret = new HashSet<DNSResult.IPFromRecord>();
        for (var type : _typesToProcessIPsFrom) {
            var ips = switch (type) {
                case "A" -> streamIfNotNull(data.A());
                case "AAAA" -> streamIfNotNull(data.AAAA());
                case "CNAME" -> data.CNAME() == null
                        ? Stream.<String>empty()
                        : streamIfNotNull(data.CNAME().relatedIps());
                case "MX" -> data.MX() == null
                        ? Stream.<String>empty()
                        : data.MX().stream().flatMap(x -> streamIfNotNull(x.relatedIps()));
                case "NS" -> data.NS() == null
                        ? Stream.<String>empty()
                        : data.NS().stream().flatMap(x -> streamIfNotNull(x.relatedIps()));
                default -> Stream.<String>empty();
            };

            ret.addAll(ips.map(ip -> new DNSResult.IPFromRecord(ip, type)).collect(Collectors.toSet()));
        }

        return ret;
    }

    @Override
    public @NotNull String getName() {
        return "dns-tls";
    }

    @Override
    public void close() {
        super.close();
        _executor.close();
    }

    protected DNSResult errorResult(int code, @NotNull String message) {
        return new DNSResult(code, message, Instant.now(), null, null, null);
    }
}
