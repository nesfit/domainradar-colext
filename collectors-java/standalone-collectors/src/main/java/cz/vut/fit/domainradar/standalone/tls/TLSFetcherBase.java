package cz.vut.fit.domainradar.standalone.tls;

import cz.vut.fit.domainradar.models.ResultCodes;
import cz.vut.fit.domainradar.models.results.TLSResult;
import cz.vut.fit.domainradar.models.tls.TLSData;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;

import javax.net.ssl.*;
import javax.security.auth.x500.X500Principal;
import java.io.*;
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
 * An abstract base class for fetching TLS information from a given host and IP address.
 *
 * <p>To use this class, the JVM must be started with the following parameter:</p>
 * <pre>-Djdk.httpclient.allowRestrictedHeaders=host</pre>
 *
 * <p>Implementations must define how to construct the SSL context and socket by implementing
 * the respective abstract methods.</p>
 */
public abstract class TLSFetcherBase implements Closeable, AutoCloseable {

    protected final Logger _logger;
    protected final int _timeout;

    public TLSFetcherBase(int timeoutMs, @NotNull Logger logger) {
        _timeout = timeoutMs;
        _logger = logger;
    }

    protected abstract SSLContext buildSSLContext() throws NoSuchAlgorithmException, KeyManagementException;

    protected abstract Socket buildSocket() throws IOException;

    /**
     * Connects to a TLS host on a given IP, using a given hostname for a SNI header.
     * Extracts TLS handshake information.
     *
     * @param hostName The target hostname.
     * @param targetIp The target IP address.
     * @return A {@link TLSResult} object with the handshake information, the server's certificate
     * and the response body.
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
                return errorResult(ResultCodes.UNSUPPORTED_ADDRESS, null);
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

                final var tlsData = new TLSData(targetIp, protocol, cipher, certificates);
                return new TLSResult(ResultCodes.OK, null, Instant.now(), tlsData);
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
        return new TLSResult(code, message, Instant.now(), null);
    }
}
