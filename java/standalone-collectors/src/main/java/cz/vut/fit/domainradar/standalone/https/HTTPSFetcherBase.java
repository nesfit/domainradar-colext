package cz.vut.fit.domainradar.standalone.https;

import cz.vut.fit.domainradar.models.ResultCodes;
import cz.vut.fit.domainradar.models.results.TLSResult;
import cz.vut.fit.domainradar.models.tls.TLSData;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;

import javax.net.ssl.*;
import javax.security.auth.x500.X500Principal;
import java.io.*;
import java.net.*;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * An abstract base class for fetching HTTPS (and optionally HTTP) content and TLS information
 * from a given host and IP address.
 *
 * <p>To use this class, the JVM must be started with the following parameter:</p>
 * <pre>-Djdk.httpclient.allowRestrictedHeaders=host</pre>
 *
 * <p>Implementations must define how to construct the HTTP client, SSL context, and socket by
 * implementing the respective abstract methods.</p>
 */
public abstract class HTTPSFetcherBase implements Closeable, AutoCloseable {

    /**
     * A set of HTTP headers to send in the request.
     */
    protected static final String HTTP_HEADERS =
            "Accept: text/html, application/xhtml+xml, application/xml\r\n" +
                    "Accept-Encoding: identity\r\n" +
                    "User-Agent: Mozilla/5.0 (X11; Linux x86_64; rv:131.0) Gecko/20100101 Firefox/131.0\r\n" +
                    "Connection: close\r\n\r\n";

    protected final Logger _logger;
    protected final int _maxRedirects;
    protected final int _timeout;
    protected HttpClient _httpClient;

    public HTTPSFetcherBase(int maxRedirects, int timeoutMs, @NotNull Logger logger) {
        _maxRedirects = maxRedirects;
        _timeout = timeoutMs;
        _logger = logger;
    }

    protected abstract HttpClient buildHttpClient();

    protected abstract SSLContext buildSSLContext() throws NoSuchAlgorithmException, KeyManagementException;

    protected abstract Socket buildSocket() throws IOException;

    @Override
    public void close() {
        if (_httpClient != null) {
            _httpClient.close();
        }
    }

    /**
     * Connects to a TLS host on a given IP, using a given hostname for a SNI header.
     * Extracts TLS handshake information. Sends an HTTP GET request for / and stores the response body.
     *
     * @param hostName The target hostname.
     * @param targetIp The target IP address.
     * @return A {@link TLSResult} object with the handshake information, the server's certificate and the
     * response body. The body may be null if the limit for redirect count has been reached or other error occurred.
     */
    public TLSResult collect(@NotNull String hostName, @NotNull String targetIp) {
        // Create a new SSL context with a naive trust manager that accepts all certificates
        SSLContext context;
        try {
            _logger.trace("[{}] Building TLS context", hostName);
            context = this.buildSSLContext();
        } catch (NoSuchAlgorithmException | KeyManagementException e) {
            _logger.error("[{}] TLS context error", hostName, e);
            // Should not happen
            return errorResult(ResultCodes.INTERNAL_ERROR, e.getMessage());
        }


        _logger.trace("[{}] Building raw socket", hostName);
        try (var rawSocket = this.buildSocket()) {
            try {
                _logger.trace("[{}] Connecting to {}:443", hostName, targetIp);
                rawSocket.connect(new InetSocketAddress(targetIp, 443), _timeout);
            } catch (SocketTimeoutException e) {
                _logger.debug("[{}] Connection timed out", hostName);
                return errorResult(ResultCodes.TIMEOUT, "Connection timed out (%d ms)".formatted(_timeout));
            } catch (IllegalArgumentException e) {
                _logger.debug("[{}] Cannot use IPv6 ({})", hostName, targetIp);
                return errorResult(ResultCodes.UNSUPPORTED_ADDRESS, "Cannot use this IP version");
            }

            // Make the TLS layer
            _logger.trace("[{}] Building TLS socket", hostName);
            SSLSocketFactory factory = context.getSocketFactory();
            try (var socket = (SSLSocket) factory.createSocket(rawSocket, targetIp, 443, false)) {
                // Enable timeouts
                socket.setSoTimeout(_timeout);

                // Enable SNI
                SSLParameters sslParams = new SSLParameters();
                sslParams.setServerNames(List.of(new SNIHostName(hostName)));
                socket.setSSLParameters(sslParams);

                // Start handshake to retrieve session details
                _logger.trace("[{}] Starting TLS handshake", hostName);
                socket.startHandshake();

                _logger.trace("[{}] Evaluating session details", hostName);
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

                _logger.trace("[{}] Starting HTTP fetch", hostName);
                @Nullable final var finalHtmlResponse = this.fetchHTTPSContent(hostName, "/", socket, context,
                        null, 0);
                final var tlsData = new TLSData(targetIp, protocol, cipher, certificates);
                return new TLSResult(ResultCodes.OK, null, Instant.now(), tlsData, finalHtmlResponse);
            } catch (SocketTimeoutException e) {
                _logger.debug("Socket read timed out: {}", hostName);
                return errorResult(ResultCodes.TIMEOUT, "Socket read timed out (%d ms)".formatted(_timeout));
            } catch (SSLHandshakeException e) {
                _logger.debug("TLS handshake error: {}: {}", hostName, e.getMessage());
                return errorResult(ResultCodes.CANNOT_FETCH, "Socket handshake error: " + e.getMessage());
            } catch (IOException e) {
                _logger.debug("TLS error: {}: {}", hostName, e.getMessage());
                return errorResult(ResultCodes.CANNOT_FETCH, e.getMessage());
            }
        } catch (IOException e) {
            _logger.debug("Cannot connect to {}: {}", hostName, e.getMessage());
            return errorResult(ResultCodes.CANNOT_FETCH, e.getMessage());
        }
    }

    public CompletableFuture<String> collectHTTPOnly(@NotNull String hostName, @NotNull String targetIp) {
        try {
            _logger.trace("[{}] Fetching content through HTTP", hostName);
            var uri = new URI("http://" + hostName);
            return this.fetchHTTPContent(uri, targetIp, 0);
        } catch (URISyntaxException e) {
            _logger.debug("[{}] Cannot create URI", hostName, e);
            return CompletableFuture.completedFuture(null);
        }
    }

    protected String fetchHTTPSContent(String hostName, String path, Socket socket, SSLContext sslContext,
                                       String referrer, int counter) throws IOException {
        _logger.trace("[{}|R:{}] Writing HTTP GET", hostName, referrer);
        var osw = new OutputStreamWriter(socket.getOutputStream());
        osw.write("GET %s HTTP/1.1\r\nHost: ".formatted(path));
        osw.write(hostName);

        if (referrer != null) {
            osw.write("\r\nReferer: https://%s/\r\n".formatted(referrer));
        } else {
            osw.write("\r\n");
        }

        osw.write(HTTP_HEADERS);
        osw.flush();

        final var reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        String line;
        String location = null;
        var isRedirect = false;
        var statusCode = 0;

        // Read HTTP headers
        _logger.trace("[{}|R:{}] Reading HTTP response", hostName, referrer);
        while ((line = reader.readLine()) != null) {
            if (line.startsWith("HTTP/")) {
                statusCode = Integer.parseInt(line.split(" ")[1]);
            }

            if (line.startsWith("Location:")) {
                // Extract the location header
                var colonIndex = line.indexOf(':');
                location = line.substring(colonIndex + 1).trim();
                isRedirect = true;
                // Stop reading headers if we find a location header
                break;
            }

            if (line.isEmpty()) break; // End of headers
        }

        // Check if it's a redirect
        if (isRedirect && statusCode >= 300 && statusCode < 400) {
            // If the maximum number of redirects has been reached, return null
            if (counter == _maxRedirects) {
                // The socket is not closed here as it will be closed by the caller.
                _logger.trace("[{}|R:{}] Maximum redirects limit reached", hostName, referrer);
                return null;
            }

            // Handle the redirect. Close the current socket first so that resources are not wasted.
            _logger.debug("[{}|R:{}] Redirecting to: '{}'", hostName, referrer, location);
            reader.close();
            socket.close();
            return this.handleRedirect(sslContext, hostName, location, counter + 1);
        }

        // Read the body of the response
        var body = new StringBuilder();
        var bodyStarted = false;

        while ((line = reader.readLine()) != null) {
            if (!bodyStarted) {
                // Skip lines until the body starts
                if (line.trim().isEmpty()) {
                    bodyStarted = true;
                    continue;
                }
                // Check for custom numeric prefixes
                try {
                    Integer.parseInt(line.trim());
                    continue;
                } catch (NumberFormatException e) {
                    bodyStarted = true;
                }
            }

            body.append(line).append("\n");
        }

        final var resultBody = body.toString();
        _logger.trace("[{}|R:{}] Body read", hostName, referrer);
        return resultBody;
    }

    protected String handleRedirect(SSLContext sslContext, String currentLocation, String newLocation, int counter) {
        // Parse the redirected URL
        String newHost, newPath;
        int port;

        try {
            var uri = new URI(newLocation);
            // Handle relative redirects
            if (!uri.isAbsolute()) {
                uri = new URI("https", currentLocation, newLocation, "");
            }
            // Check if URI is HTTPS
            if (!uri.getScheme().equalsIgnoreCase("https")) {
                _logger.debug("[{}] Target is not HTTPS: {}", currentLocation, uri);
                return null;
            }
            newHost = uri.getHost();
            var query = uri.getRawQuery();
            newPath = uri.getPath() + (query != null ? "?" + query : "");
            port = uri.getPort();
            if (port == -1)
                port = 443;
        } catch (URISyntaxException | IllegalArgumentException e) {
            _logger.debug("[{}->{}] Bad redirect URL", currentLocation, newLocation, e);
            return null;
        }

        _logger.trace("[{}->{}] Building TLS socket", currentLocation, newLocation);
        try (var socket = (SSLSocket) sslContext.getSocketFactory().createSocket()) {
            _logger.trace("[{}->{}] Connecting", currentLocation, newLocation);
            socket.connect(new InetSocketAddress(newHost, port), _timeout);
            socket.setSoTimeout(_timeout);
            socket.startHandshake();
            return this.fetchHTTPSContent(newHost, newPath, socket, sslContext, currentLocation, counter);
        } catch (IOException e) {
            _logger.debug("[{}->{}] Cannot connect", currentLocation, newLocation, e);
            return null;
        }
    }

    private static TLSData.Certificate parseCertificate(X509Certificate cert) {
        X500Principal subject = cert.getSubjectX500Principal();
        String subjectDN = subject.getName(X500Principal.RFC1779);

        try {
            return new TLSData.Certificate(subjectDN, cert.getEncoded());
        } catch (CertificateEncodingException e) {
            return new TLSData.Certificate(subjectDN, new byte[0]);
        }
    }

    protected CompletableFuture<String> fetchHTTPContent(@NotNull URI location,
                                                         @Nullable String targetIp, int counter) {
        HttpRequest.Builder reqBuilder = HttpRequest.newBuilder()
                .timeout(Duration.ofMillis(_timeout))
                .header("Accept", "*/*")
                .GET();

        // If the target IP is provided, we need to modify the URI to use it
        var host = location.getHost();
        if (targetIp != null) {
            try {
                // Create a new URI with the target IP as the host
                location = new URI(location.getScheme(), targetIp, location.getPath(), null);
            } catch (URISyntaxException e) {
                _logger.debug("[{}] Cannot construct new target URI", host, e);
                return null;
            }
            // Set the Host header to the original host name
            // This requires the -Djdk.httpclient.allowRestrictedHeaders=host JVM property to be set
            _logger.trace("[{}] Changing target URI to '{}', using Host header", host, location);
            reqBuilder = reqBuilder
                    .uri(location)
                    .header("Host", host);
        } else {
            // If no target IP is provided, use the original URI
            _logger.trace("[{}] No target IP provided, using the hostname", host);
            reqBuilder = reqBuilder
                    .uri(location);
        }

        final var finalLocation = location;

        // Build HTTP client if not already built
        if (_httpClient == null) {
            _logger.trace("[{}] Building HTTPClient", host);
            _httpClient = this.buildHttpClient();
            if (_httpClient == null) {
                _logger.error("HTTP client is not initialized");
                return CompletableFuture.completedFuture(null);
            }
        }

        // Send the request asynchronously
        _logger.trace("[{}] Fetching HTTP content through HTTPClient", host);
        var responseFuture = _httpClient.sendAsync(reqBuilder.build(), HttpResponse.BodyHandlers.ofString());
        return responseFuture.thenCompose(response -> {
            // Check if the response is a redirect
            if (response.statusCode() >= 300 && response.statusCode() < 400) {
                // If the maximum number of redirects has been reached, return null
                if (counter == _maxRedirects) {
                    _logger.trace("[{}] Maximum redirects limit reached", host);
                    return CompletableFuture.completedFuture(null);
                }

                // Handle the redirect
                var newLocation = response.headers().firstValue("Location").orElse(null);
                if (newLocation == null) {
                    _logger.trace("[{}] Redirect status code {} but no Location header found", host,
                            response.statusCode());
                    return CompletableFuture.completedFuture(null);
                }

                try {
                    var uri = new URI(newLocation);
                    // Handle relative redirects
                    if (!uri.isAbsolute()) {
                        uri = new URI(finalLocation.getScheme(), host, newLocation, "");
                        // Use the original target IP for relative redirects
                        return this.fetchHTTPContent(uri, targetIp, counter + 1);
                    } else {
                        // If the URI is absolute, just use it; the target IP does not matter anymore
                        return this.fetchHTTPContent(uri, null, counter + 1);
                    }
                } catch (URISyntaxException | IllegalArgumentException e) {
                    _logger.debug("[{}] Cannot create URI", host, e);
                    return CompletableFuture.completedFuture(null);
                }
            }

            return CompletableFuture.completedFuture(response.body());
        });
    }

    public static TLSResult errorResult(int code, String message) {
        return new TLSResult(code, message, Instant.now(), null, null);
    }

}
