package cz.vut.fit.domainradar.standalone.https;

import org.slf4j.Logger;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import java.io.IOException;
import java.net.Socket;
import java.net.http.HttpClient;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.concurrent.ExecutorService;

public class HTTPSFetcherImpl extends HTTPSFetcherBase {

    private final ExecutorService _executor;

    public HTTPSFetcherImpl(int maxRedirects, int timeoutMs, ExecutorService executor, Logger logger) {
        super(maxRedirects, timeoutMs, logger);
        _executor = executor;
    }

    @Override
    protected HttpClient buildHttpClient() {
        return HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.NEVER)
                .connectTimeout(Duration.ofMillis(_timeout))
                .version(HttpClient.Version.HTTP_1_1)
                .executor(_executor)
                .build();
    }

    @Override
    protected SSLContext buildSSLContext() throws NoSuchAlgorithmException, KeyManagementException {
        var context = SSLContext.getInstance("TLS");
        context.init(null, new TrustManager[]{new NaiveTrustManager()}, null);
        return context;
    }

    @Override
    protected Socket buildSocket() throws IOException {
        return new Socket();
    }
}
