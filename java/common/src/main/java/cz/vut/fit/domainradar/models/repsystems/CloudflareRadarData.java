package cz.vut.fit.domainradar.models.repsystems;

/**
 * A record that represents a set of data retrieved from the Cloudflare Radar system about a specific IP address
 * or domain name.
 *
 * @author Matěj Čech
 */
public record CloudflareRadarData(
        Boolean malicious
) {
}
