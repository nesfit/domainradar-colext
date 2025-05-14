package cz.vut.fit.domainradar.models.repsystems;

/**
 * A record that represents a set of data retrieved from the ProjectHoneypot system about a specific IP address.
 *
 * @author Matěj Čech
 */
public record ProjectHoneypotData(
        Boolean malicious
) {
}
