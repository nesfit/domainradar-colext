package cz.vut.fit.domainradar.models;

/**
 * A record that represents a pair of a domain name and an IP address.
 *
 * @param dn The domain name.
 * @param ip The IP address.
 * @author Ondřej Ondryáš
 */
public record IPToProcess(String dn, String ip) {
}
