package cz.vut.fit.domainradar.standalone.collectors;

import cz.vut.fit.domainradar.Common;
import cz.vut.fit.domainradar.models.ResultCodes;
import cz.vut.fit.domainradar.models.results.TLSResult;
import cz.vut.fit.domainradar.models.tls.TLSData;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.net.ssl.*;
import javax.security.auth.x500.X500Principal;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.*;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * This class encapsulates the processing logic for the {@link TLSCollector}.
 */
public class TLSCollectorImpl {

    /**
     * A set of HTTP headers to send in the request.
     */
    private static final String HTTP_HEADERS =
            "Accept: text/html, application/xhtml+xml, application/xml\r\n" +
                    "Accept-Encoding: identity\r\n" +
                    "User-Agent: Mozilla/5.0 (X11; Linux x86_64; rv:131.0) Gecko/20100101 Firefox/131.0\r\n" +
                    "Connection: close\r\n\r\n";

    private static final org.slf4j.Logger Logger = Common.getComponentLogger(TLSCollector.class);

    private final int _maxRedirects;
    private final int _timeout;

    public TLSCollectorImpl(int maxRedirects, int timeoutMs) {
        _maxRedirects = maxRedirects;
        _timeout = timeoutMs;
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
            context = SSLContext.getInstance("TLS");
            context.init(null, new TrustManager[]{new TLSCollector.NaiveTrustManager()}, null);
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

                @Nullable final var finalHtmlResponse = this.fetchHTTPContent(hostName, socket, context,
                        null, 0);
                final var tlsData = new TLSData(targetIp, protocol, cipher, certificates);
                return new TLSResult(ResultCodes.OK, null, Instant.now(), tlsData, finalHtmlResponse);
            } catch (SocketTimeoutException e) {
                Logger.debug("Socket read timed out: {}", hostName);
                return errorResult(ResultCodes.TIMEOUT, "Socket read timed out (%d ms)".formatted(_timeout));
            } catch (SSLHandshakeException e) {
                Logger.debug("TLS handshake error: {}: {}", hostName, e.getMessage());
                return errorResult(ResultCodes.CANNOT_FETCH, "Socket handshake error: " + e.getMessage());
            } catch (IOException e) {
                Logger.debug("TLS error: {}: {}", hostName, e.getMessage());
                return errorResult(ResultCodes.CANNOT_FETCH, e.getMessage());
            }
        } catch (IOException e) {
            Logger.debug("Cannot connect to {}: {}", hostName, e.getMessage());
            return errorResult(ResultCodes.CANNOT_FETCH, e.getMessage());
        }
    }

    private String fetchHTTPContent(String hostName, SSLSocket socket, SSLContext sslContext,
                                    String referrer, int counter) throws IOException {
        var osw = new OutputStreamWriter(socket.getOutputStream());
        osw.write("GET / HTTP/1.1\r\nHost: ");
        osw.write(hostName);

        if (referrer != null) {
            osw.write("\r\nReferer: https://" + referrer + "/\r\n");
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
        while ((line = reader.readLine()) != null) {
            if (line.startsWith("HTTP/")) {
                statusCode = Integer.parseInt(line.split(" ")[1]);
            }

            if (line.startsWith("Location:")) {
                location = line.split(" ")[1];
                isRedirect = true;
            }

            if (line.isEmpty()) break; // End of headers
        }

        // Check if it's a redirect
        if (isRedirect && statusCode >= 300 && statusCode < 400 && location != null) {
            // If the maximum number of redirects has been reached, return null
            if (counter == _maxRedirects)
                // The socket is not closed here as it will be closed by the caller.
                return null;

            // Handle the redirect. Close the current socket first so that resources are not wasted.
            Logger.debug("Redirecting from '{}' to: '{}'", hostName, location);
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
        Logger.trace("Body read");
        return resultBody;
    }

    private String handleRedirect(SSLContext sslContext, String currentLocation, String newLocation, int counter) {
        // Parse the redirected URL
        String newHost;
        int port;

        try {
            var uri = new URI(newLocation);
            // Handle relative redirects
            if (!uri.isAbsolute()) {
                uri = new URI("https://", currentLocation, newLocation, "");
            }

            final var url = uri.toURL();
            newHost = url.getHost();
            port = url.getPort();
            if (port == -1)
                port = 443;
        } catch (MalformedURLException | URISyntaxException | IllegalArgumentException e) {
            return null;
        }

        try (var socket = (SSLSocket) sslContext.getSocketFactory().createSocket()) {
            socket.connect(new InetSocketAddress(newHost, port), _timeout);
            socket.setSoTimeout(_timeout);
            socket.startHandshake();
            return this.fetchHTTPContent(newHost, socket, sslContext, currentLocation, counter + 1);
        } catch (IOException e) {
            Logger.debug("Cannot connect to {}: {}", newLocation, e.getMessage());
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

    public static TLSResult errorResult(int code, String message) {
        return new TLSResult(code, message, Instant.now(), null, null);
    }

}
