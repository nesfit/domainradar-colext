package cz.vut.fit.domainradar.standalone.https;

import cz.vut.fit.domainradar.models.ResultCodes;
import cz.vut.fit.domainradar.models.results.TLSResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.Logger;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class HTTPSFetcherImplTest {

    private HTTPSFetcherImpl fetcher;
    private HttpClient mockHttpClient;
    private HttpResponse<Object> mockResponse1;
    private HttpResponse<Object> mockResponse2;
    private final Logger dummyLogger = org.slf4j.LoggerFactory.getLogger("test");

    @BeforeEach
    void setUp() throws Exception {
        mockHttpClient = mock(HttpClient.class);
        mockResponse1 = mock(HttpResponse.class);
        mockResponse2 = mock(HttpResponse.class);

        // Subclass implementation so we can inject our mockHttpClient
        fetcher = new HTTPSFetcherImpl(2, 1_000,
                Executors.newSingleThreadExecutor(),
                dummyLogger) {
            @Override
            protected HttpClient buildHttpClient() {
                return mockHttpClient;
            }
        };
    }

    @Test
    void errorResult_static() {
        TLSResult er = HTTPSFetcherBase.errorResult(ResultCodes.INTERNAL_ERROR, "boom");
        assertEquals(ResultCodes.INTERNAL_ERROR, er.statusCode());
        assertEquals("boom", er.error());
        assertNull(er.tlsData());
        assertNull(er.html());
        // timestamp is “now-ish”
        assertFalse(er.lastAttempt().isBefore(Instant.now().minusSeconds(5)));
    }

    //–– collectHTTPOnly(...) tests ––

    @Test
    void collectHTTPOnly_200NoRedirect() throws Exception {
        when(mockResponse1.statusCode()).thenReturn(200);
        when(mockResponse1.body()).thenReturn("OK");
        when(mockResponse1.headers()).thenReturn(
                HttpHeaders.of(Collections.emptyMap(), (a, b) -> true)
        );
        when(mockHttpClient.sendAsync(any(), any()))
                .thenReturn(CompletableFuture.completedFuture(mockResponse1));

        String body = fetcher.collectHTTPOnly("example.com", "1.1.1.1")
                .get(1, TimeUnit.SECONDS);
        assertEquals("OK", body);

        // verify the request was to http://example.com, with Host header
        ArgumentCaptor<HttpRequest> cap = ArgumentCaptor.forClass(HttpRequest.class);
        verify(mockHttpClient).sendAsync(cap.capture(), any());
        assertEquals(URI.create("http://1.1.1.1"), cap.getValue().uri());
        assertEquals("example.com",
                cap.getValue().headers().firstValue("Host").orElse(""));
        assertEquals("*/*",
                cap.getValue().headers().firstValue("Accept").orElse(""));
    }

    @Test
    void collectHTTPOnly_redirectsOnceThen200() throws Exception {
        // first 302
        when(mockResponse1.statusCode()).thenReturn(302);
        when(mockResponse1.headers()).thenReturn(
                HttpHeaders.of(
                        Map.of("Location", List.of("http://example.com/final")),
                        (a, b) -> true
                )
        );
        // then 200
        when(mockResponse2.statusCode()).thenReturn(200);
        when(mockResponse2.body()).thenReturn("DONE");
        when(mockResponse2.headers()).thenReturn(
                HttpHeaders.of(Collections.emptyMap(), (a, b) -> true)
        );

        when(mockHttpClient.sendAsync(any(), any()))
                .thenReturn(
                        CompletableFuture.completedFuture(mockResponse1),
                        CompletableFuture.completedFuture(mockResponse2)
                );

        String body = fetcher.collectHTTPOnly("example.com", "1.1.1.1")
                .get(1, TimeUnit.SECONDS);
        assertEquals("DONE", body);
    }

    @Test
    void collectHTTPOnly_exceedMaxRedirects_returnsNull() throws Exception {
        // shrink redirects to zero
        HTTPSFetcherImpl zero = new HTTPSFetcherImpl(0, 1_000,
                Executors.newSingleThreadExecutor(), dummyLogger) {
            @Override
            protected HttpClient buildHttpClient() {
                return mockHttpClient;
            }
        };
        when(mockResponse1.statusCode()).thenReturn(301);
        when(mockResponse1.headers()).thenReturn(
                HttpHeaders.of(Map.of("Location", List.of("/whatever")), (a, b) -> true)
        );
        when(mockHttpClient.sendAsync(any(), any()))
                .thenReturn(CompletableFuture.completedFuture(mockResponse1));

        String body = zero.collectHTTPOnly("example.com", "1.1.1.1")
                .get(1, TimeUnit.SECONDS);
        assertNull(body, "should give up after maxRedirects=0");
    }

    @Test
    void collectHTTPOnly_withTargetIp() throws Exception {
        when(mockResponse1.statusCode()).thenReturn(200);
        when(mockResponse1.body()).thenReturn("IPBODY");
        when(mockResponse1.headers()).thenReturn(
                HttpHeaders.of(Collections.emptyMap(), (a, b) -> true)
        );
        when(mockHttpClient.sendAsync(any(), any()))
                .thenReturn(CompletableFuture.completedFuture(mockResponse1));

        String body = fetcher.collectHTTPOnly("domain.test", "1.2.3.4")
                .get(1, TimeUnit.SECONDS);
        assertEquals("IPBODY", body);

        ArgumentCaptor<HttpRequest> cap = ArgumentCaptor.forClass(HttpRequest.class);
        verify(mockHttpClient).sendAsync(cap.capture(), any());
        HttpRequest r = cap.getValue();
        assertEquals(URI.create("http://1.2.3.4"), r.uri(),
                "URI must use the targetIp");
        assertEquals("domain.test", r.headers().firstValue("Host").orElse(""),
                "Host header remains original hostname");
    }

    //–– collect(...) error‐paths ––

    @Test
    void collect_buildSSLContextFails_returnsInternalError() {
        HTTPSFetcherImpl badCtx = new HTTPSFetcherImpl(1, 500,
                Executors.newSingleThreadExecutor(), dummyLogger) {
            @Override
            protected SSLContext buildSSLContext() throws NoSuchAlgorithmException {
                throw new NoSuchAlgorithmException("nope");
            }
        };

        TLSResult res = badCtx.collect("h", "1.1.1.1");
        assertEquals(ResultCodes.INTERNAL_ERROR, res.statusCode());
        assertTrue(res.error().contains("nope"));
    }

    @Test
    void collect_buildSocketFails_returnsCannotFetch() {
        HTTPSFetcherImpl badSock = new HTTPSFetcherImpl(1, 500,
                Executors.newSingleThreadExecutor(), dummyLogger) {
            @Override
            protected Socket buildSocket() throws IOException {
                throw new IOException("socket bad");
            }
        };
        TLSResult r = badSock.collect("h", "1.2.3.4");
        assertEquals(ResultCodes.CANNOT_FETCH, r.statusCode());
        assertTrue(r.error().contains("socket bad"));
    }

    @Test
    void collect_socketConnectTimeout_returnsTimeout() {
        HTTPSFetcherImpl tmo = new HTTPSFetcherImpl(1, 50,
                Executors.newSingleThreadExecutor(), dummyLogger) {
            @Override
            protected Socket buildSocket() {
                return new Socket() {
                    public void connect(SocketAddress addr, int to) throws IOException {
                        throw new SocketTimeoutException("timed");
                    }
                };
            }
        };
        TLSResult r = tmo.collect("h", "8.8.8.8");
        assertEquals(ResultCodes.TIMEOUT, r.statusCode());
        assertTrue(r.error().contains("timed"));
    }

    @Test
    void collect_socketConnectIllegalArg_returnsUnsupportedAddress() {
        HTTPSFetcherImpl ill = new HTTPSFetcherImpl(1, 50,
                Executors.newSingleThreadExecutor(), dummyLogger) {
            @Override
            protected Socket buildSocket() {
                return new Socket() {
                    @Override
                    public void connect(SocketAddress addr, int to) {
                        throw new IllegalArgumentException("bad ip");
                    }
                };
            }
        };
        TLSResult r = ill.collect("h", ":::1");
        assertEquals(ResultCodes.UNSUPPORTED_ADDRESS, r.statusCode());
    }
}
