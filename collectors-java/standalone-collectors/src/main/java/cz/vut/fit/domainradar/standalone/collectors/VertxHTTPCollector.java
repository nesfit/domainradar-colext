package cz.vut.fit.domainradar.standalone.collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.net.InetAddresses;
import cz.vut.fit.domainradar.CollectorConfig;
import cz.vut.fit.domainradar.Common;
import cz.vut.fit.domainradar.Topics;
import cz.vut.fit.domainradar.models.ResultCodes;
import cz.vut.fit.domainradar.models.results.HTTPResult;
import cz.vut.fit.domainradar.serialization.JsonSerde;
import cz.vut.fit.domainradar.standalone.BaseStandaloneCollector;
import io.confluent.parallelconsumer.vertx.VertxParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.vertx.VertxParallelStreamProcessor;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClosedException;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import org.apache.commons.cli.CommandLine;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jetbrains.annotations.NotNull;
import pl.tlinkowski.unij.api.UniLists;

import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A collector that fetches HTML from a website root using Vertx WebClient.
 *
 * @author Ondřej Ondryáš
 */
public class VertxHTTPCollector
        extends BaseStandaloneCollector<String, String, VertxParallelStreamProcessor<String, String>> {
    public static final String NAME = "http";
    public static final String COMPONENT_NAME = "collector-" + NAME;
    private static final org.slf4j.Logger Logger = Common.getComponentLogger(VertxHTTPCollector.class);

    private final KafkaProducer<String, HTTPResult> _producer;
    private final int _timeoutMs;
    private final int _maxRedirects;
    private final int _httpPort;
    private final int _httpsPort;
    private final int _maxSizeBytes = 1024 * 1024; // 1 MB (TODO: Configuration property)

    private WebClient _client;

    private record FetchResult(int httpCode, String targetUrl, byte[] body, boolean truncated) {
    }

    private record RedirectTarget(String scheme, String pathWithQuery) {
    }

    public VertxHTTPCollector(@NotNull ObjectMapper jsonMapper, @NotNull String appName,
                              @NotNull Properties properties) {
        super(jsonMapper, appName, properties, Serdes.String(), Serdes.String());

        _timeoutMs = Integer.parseInt(properties.getProperty(CollectorConfig.HTTP_TIMEOUT_MS_CONFIG,
                CollectorConfig.HTTP_TIMEOUT_MS_DEFAULT));
        _maxRedirects = Integer.parseInt(properties.getProperty(CollectorConfig.HTTP_MAX_REDIRECTS_CONFIG,
                CollectorConfig.HTTP_MAX_REDIRECTS_DEFAULT));
        _httpPort = Integer.parseInt(properties.getProperty(CollectorConfig.HTTP_PORT_CONFIG,
                CollectorConfig.HTTP_PORT_DEFAULT));
        _httpsPort = Integer.parseInt(properties.getProperty(CollectorConfig.HTTPS_PORT_CONFIG,
                CollectorConfig.HTTPS_PORT_DEFAULT));

        _producer = super.createProducer(new StringSerializer(),
                JsonSerde.of(jsonMapper, HTTPResult.class).serializer());
    }

    @Override
    public void run(CommandLine cmd) {
        final var processingTimeout = (long) (_timeoutMs * (_maxRedirects + 1) * 1.2);
        buildProcessor(0, processingTimeout);

        final Object vertxObj;
        try {
            final var vertxField = VertxParallelEoSStreamProcessor.class
                    .getDeclaredField("vertx");
            vertxField.setAccessible(true);
            vertxObj = vertxField.get(_parallelProcessor);
        } catch (IllegalArgumentException | IllegalAccessException | NoSuchFieldException e) {
            Logger.error("Reflection error: cannot retrieve Vertx instance from parallel processor", e);
            return;
        }

        final var vertx = (Vertx) vertxObj;
        final var timeoutMs = (int) (_timeoutMs * 3L / 4);
        WebClientOptions webClientOptions = new WebClientOptions()
                .setMaxPoolSize(_maxConcurrency)
                .setHttp2MaxPoolSize(_maxConcurrency)
                .setIdleTimeoutUnit(TimeUnit.MILLISECONDS)
                .setIdleTimeout(timeoutMs)
                .setConnectTimeout(timeoutMs)
                .setReadIdleTimeout(timeoutMs)
                .setSslHandshakeTimeout(timeoutMs)
                .setSslHandshakeTimeoutUnit(TimeUnit.MILLISECONDS)
                .setTrustAll(true)
                .setVerifyHost(false);

        _client = WebClient.create(vertx, webClientOptions);

        _parallelProcessor.subscribe(UniLists.of(Topics.IN_HTTP));
        _parallelProcessor.vertxFuture(context -> {
            final List<Future<Void>> futures = new ArrayList<>();

            context.getConsumerRecordsFlattened().forEach(record -> {
                final var dn = record.key();
                final var ip = record.value();
                futures.add(this.processEntry(dn, ip));
            });

            if (futures.isEmpty()) {
                return Future.<Void>succeededFuture();
            }

            return Future.all(futures).mapEmpty();
        });
    }

    private Future<Void> processEntry(String domainName, String ip) {
        if (domainName == null || domainName.isBlank()) {
            _producer.send(resultRecord(Topics.OUT_HTTP, domainName,
                    errorResult(ResultCodes.INVALID_DOMAIN_NAME, "Domain name is missing.")));
            return Future.succeededFuture();
        }

        if (ip == null || ip.isBlank()) {
            _producer.send(resultRecord(Topics.OUT_HTTP, domainName,
                    errorResult(ResultCodes.INVALID_ADDRESS, "IP address is missing.")));
            return Future.succeededFuture();
        }

        try {
            InetAddresses.forString(ip);
        } catch (IllegalArgumentException e) {
            _producer.send(resultRecord(Topics.OUT_HTTP, domainName,
                    errorResult(ResultCodes.INVALID_ADDRESS, e.getMessage())));
            return Future.succeededFuture();
        }

        return fetchWithFallback(domainName, ip)
                .compose(result -> {
                    _producer.send(resultRecord(Topics.OUT_HTTP, domainName, result));
                    return Future.<Void>succeededFuture();
                })
                .recover(cause -> {
                    var resultCode = getResultCodeForError(cause);
                    _producer.send(resultRecord(Topics.OUT_HTTP, domainName,
                            errorResult(resultCode, cause.getMessage())));
                    return Future.succeededFuture();
                });
    }

    private Future<HTTPResult> fetchWithFallback(String domainName, String ip) {
        return fetchWithRedirects("https", domainName, ip, _maxRedirects)
                .map(VertxHTTPCollector::successResult)
                .recover(cause -> {
                    Logger.debug("HTTPS failed for {} at {}: {}", domainName, ip, cause.getMessage());
                    return fetchWithRedirects("http", domainName, ip, _maxRedirects)
                            .map(VertxHTTPCollector::successResult);
                });
    }

    private Future<FetchResult> fetchWithRedirects(String scheme, String domainName, String ip, int redirectsLeft) {
        return fetchWithRedirects(scheme, domainName, ip, "/", redirectsLeft);
    }

    private Future<FetchResult> fetchWithRedirects(String scheme, String domainName, String ip,
                                                   String pathWithQuery, int redirectsLeft) {
        final var port = schemeToPort(scheme);
        final var url = buildUrl(scheme, ip, port, pathWithQuery);

        final HttpRequest<io.vertx.core.buffer.Buffer> request = _client.getAbs(url);
        request.virtualHost(domainName);
        request.timeout(_timeoutMs);

        return request.send().compose(response -> {
            final var status = response.statusCode();
            if (isRedirect(status) && redirectsLeft > 0) {
                final var location = response.getHeader("Location");
                if (location != null && !location.isBlank()) {
                    RedirectTarget target = null;
                    try {
                        target = resolveRedirect(url, location, scheme);
                    } catch (IllegalArgumentException e) {
                        Logger.debug("Invalid redirect location for {}: {}", url, location);
                    }

                    if (target != null) {
                        return fetchWithRedirects(target.scheme(), domainName, ip, target.pathWithQuery(),
                                redirectsLeft - 1);
                    }
                }
            }

            final var buffer = response.body();
            final var truncated = buffer.length() > _maxSizeBytes;
            final var body = response.body().getBytes(0, _maxSizeBytes);
            return Future.succeededFuture(new FetchResult(status, url, body, truncated));
        });
    }

    private static RedirectTarget resolveRedirect(String currentUrl, String location, String currentScheme) {
        final URI base = URI.create(currentUrl);
        final URI resolved = base.resolve(location);
        final var scheme = resolved.getScheme() == null ? currentScheme : resolved.getScheme();

        var path = resolved.getRawPath();
        if (path == null || path.isBlank()) {
            path = "/";
        }
        final var query = resolved.getRawQuery();
        if (query != null && !query.isBlank()) {
            path = path + "?" + query;
        }

        return new RedirectTarget(scheme, path);
    }

    private static String buildUrl(String scheme, String ip, int port, String pathWithQuery) {
        final var host = formatIpForUri(ip);
        var path = pathWithQuery == null || pathWithQuery.isBlank() ? "/" : pathWithQuery;
        if (!path.startsWith("/")) {
            path = "/" + path;
        }
        return scheme + "://" + host + ":" + port + path;
    }

    private static String formatIpForUri(String ip) {
        if (ip.contains(":") && !(ip.startsWith("[") && ip.endsWith("]"))) {
            return "[" + ip + "]";
        }
        return ip;
    }

    private static boolean isRedirect(int statusCode) {
        return statusCode == 301 || statusCode == 302 || statusCode == 303
                || statusCode == 307 || statusCode == 308;
    }

    private int schemeToPort(String scheme) {
        return "https".equalsIgnoreCase(scheme) ? _httpsPort : _httpPort;
    }

    private static HTTPResult successResult(FetchResult result) {
        return new HTTPResult(ResultCodes.OK, null, Instant.now(),
                result.targetUrl, result.httpCode, result.truncated, result.body);
    }

    private static HTTPResult errorResult(int code, String message) {
        return new HTTPResult(code, message, Instant.now(), null, 0, false, null);
    }

    protected void buildProcessor(int batchSize, long timeoutMs) {
        _parallelProcessor = VertxParallelStreamProcessor.createEosStreamProcessor(
                this.makeProcessorOptionsBuilder(batchSize, timeoutMs)
                        .build());
    }

    @Override
    public @NotNull String getName() {
        return NAME;
    }

    @Override
    public void close() throws java.io.IOException {
        super.close();
        _producer.close(_closeTimeout);
        if (_client != null) {
            _client.close();
        }
    }

    private static int getResultCodeForError(Throwable cause) {
        if (cause instanceof TimeoutException) {
            return ResultCodes.TIMEOUT;
        }
        if (cause instanceof HttpClosedException) {
            return ResultCodes.CANNOT_FETCH;
        }
        return ResultCodes.CANNOT_FETCH;
    }
}
