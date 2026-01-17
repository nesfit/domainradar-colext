package cz.vut.fit.domainradar.models.ip;

/**
 * A record that represents a set of data retrieved from the NERD reputation system about a specific IP address.
 *
 * @author Ondřej Ondryáš
 */
public record NERDData(
        double reputation
) {
}
