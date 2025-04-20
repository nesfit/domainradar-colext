package cz.vut.fit.domainradar.models.repsystems;

import java.util.List;

/**
 * A record that represents a set of data retrieved from the Opentip Kaspersky system about a specific IP address
 * or domain name.
 *
 * @author Matěj Čech
 */
public record OpentipKasperskyData(
        String zone,
        List<String> categories,
        List<String> categoryZones
) {
}
