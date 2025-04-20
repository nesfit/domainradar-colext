package cz.vut.fit.domainradar.models.repsystems;

/**
 * A record that represents a set of data retrieved from the CriminalIP system about a specific IP address.
 *
 * @author Matěj Čech
 */
public record CriminalIPData(
        String scoreInbound,
        String scoreOutbound
) {
}
