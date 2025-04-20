package cz.vut.fit.domainradar.models.repsystems;

/**
 * A record that represents a set of data retrieved from the Greynoise system about a specific IP address.
 *
 * @author Matěj Čech
 */
public record GreynoiseData(
        Boolean noise,
        Boolean riot,
        String classification
) {
}
