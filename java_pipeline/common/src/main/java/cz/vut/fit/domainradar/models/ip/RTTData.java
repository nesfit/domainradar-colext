package cz.vut.fit.domainradar.models.ip;

public record RTTData(
        double min,
        double avg,
        double max,
        int sent,
        int received,
        double jitter
) {
}
