package cz.vut.fit.domainradar.models.repsystems;

import java.util.List;

/**
 * A record that represents a set of data retrieved from the Threatfox system about a specific IP address
 * or domain name.
 *
 * @author Matěj Čech
 */
public record ThreatfoxData(
        String threatType,
        String malware,
        Integer confidenceLevel,
        List<String> tags
) {
}
