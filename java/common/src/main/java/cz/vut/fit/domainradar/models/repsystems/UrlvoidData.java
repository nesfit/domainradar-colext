package cz.vut.fit.domainradar.models.repsystems;

/**
 * A record that represents a set of data retrieved from the URLVoid system about a specific domain name.
 *
 * @author Matěj Čech
 */
public record UrlvoidData(
        Integer detectionCounts
) {
}
