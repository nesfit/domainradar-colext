package cz.vut.fit.domainradar.standalone.https;

import javax.net.ssl.X509TrustManager;
import java.security.cert.X509Certificate;

/**
 * A naive trust manager that accepts all certificates.
 */
public class NaiveTrustManager implements X509TrustManager {
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