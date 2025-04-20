package cz.vut.fit.domainradar.models.repsystems;

/**
 * A record that represents a set of data retrieved from the HybridAnalysis reputation system about a specific IP address
 *
 * @author Matěj Čech
 */
public record HybridAnalysisData(
        int maliciousCnt,
        int suspiciousCnt,
        int noThreatCnt,
        int whitelistedCnt,
        int worstScore,
        int bestScore,
        int avgScore,
        int nullScoreCnt
) {
}
