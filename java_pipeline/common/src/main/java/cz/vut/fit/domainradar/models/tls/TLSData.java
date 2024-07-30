package cz.vut.fit.domainradar.models.tls;

import org.jetbrains.annotations.NotNull;

import java.util.List;

/**
 * A record representing the data collected by the TLS collector.
 *
 * @param fromIP       The IP address from which the data was collected.
 * @param protocol     The protocol used for the connection.
 * @param cipher       The cipher suite used for the connection.
 * @param certificates The certificates used for the connection.
 * @author Ondřej Ondryáš
 */
public record TLSData(@NotNull String fromIP,
                      @NotNull String protocol,
                      @NotNull String cipher,
                      @NotNull List<Certificate> certificates) {

    /**
     * A record representing a certificate used in the TLS connection.
     *
     * @param dn      The distinguished name of the certificate.
     * @param derData The DER-encoded data of the certificate.
     */
    public record Certificate(@NotNull String dn,
                              byte[] derData) {
    }

}
