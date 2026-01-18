package cz.vut.fit.domainradar.standalone.tls;

import org.slf4j.Logger;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import java.io.IOException;
import java.net.Socket;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;

public class TLSFetcherImpl extends TLSFetcherBase {


    public TLSFetcherImpl(int timeoutMs, Logger logger) {
        super(timeoutMs, logger);
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

    @Override
    public void close() {
    }
}
