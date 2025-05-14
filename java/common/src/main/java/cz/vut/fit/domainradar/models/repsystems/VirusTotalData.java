package cz.vut.fit.domainradar.models.repsystems;

/**
 * A record that represents a set of data retrieved from the VirusTotal reputation system about a specific IP address
 * or domain name.
 *
 * @author Matěj Čech
 */
public record VirusTotalData(
        int reputation,
        int malicious,
        int suspicious,
        int undetected,
        int harmless
) {
}
