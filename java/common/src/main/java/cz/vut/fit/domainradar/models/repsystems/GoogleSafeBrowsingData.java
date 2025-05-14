package cz.vut.fit.domainradar.models.repsystems;

/**
 * A record that represents a set of data retrieved from the Google Safe Browsing system about a specific IP address
 * or domain name.
 *
 * @author Matěj Čech
 */
public record GoogleSafeBrowsingData(
        Integer threatTypeUnspecifiedCnt,
        Integer malwareCnt,
        Integer socialEngineeringCnt,
        Integer unwantedSoftwareCnt,
        Integer potentiallyHarmfulApplicationCnt
) {
}
