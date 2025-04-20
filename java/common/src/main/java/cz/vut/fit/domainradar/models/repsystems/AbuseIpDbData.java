package cz.vut.fit.domainradar.models.repsystems;

/**
 * A record that represents a set of data retrieved from the AbuseIPDB reputation system about a specific IP address.
 *
 * @author Matěj Čech
 */
public record AbuseIpDbData(
        int abuseConfidenceScore,
        Boolean isWhitelisted,
        Boolean isTor,
        int totalReports
) {
}
