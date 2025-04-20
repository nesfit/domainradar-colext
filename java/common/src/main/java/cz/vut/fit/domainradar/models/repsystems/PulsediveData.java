package cz.vut.fit.domainradar.models.repsystems;

/**
 * A record that represents a set of data retrieved from the Pulsedive system about a specific IP address.
 *
 * @author Matěj Čech
 */
public record PulsediveData(
        String risk,
        String riskRecommended,
        int manualRisk
) {
}
