package cz.vut.fit.domainradar;

import cz.vut.fit.domainradar.models.ResultCodes;
import cz.vut.fit.domainradar.models.results.Result;

public final class ResultFitnessComparator {
    public static boolean isMoreUseful(final Result previous, final Result current) {
        final var oldNOK = previous.statusCode() != ResultCodes.OK;
        final var newOK = current.statusCode() == ResultCodes.OK;
        final var newNewer = previous.lastAttempt().isBefore(current.lastAttempt());
        return (oldNOK && newNewer) || (oldNOK && newOK) || (newOK && newNewer);
    }

    public static boolean isMoreUseful(final KafkaEntry previous, final KafkaEntry current) {
        if (previous == null)
            return true;
        if (current == null)
            return false;

        final var oldNOK = previous.getStatusCode() != ResultCodes.OK;
        final var newOK = current.getStatusCode() == ResultCodes.OK;
        final var newNewer = previous.getTimestamp() < current.getTimestamp();
        return (oldNOK && newNewer) || (oldNOK && newOK) || (newOK && newNewer);
    }

    public static <T extends KafkaEntry> T getMoreUseful(final T left, final T right) {
        return isMoreUseful(left, right) ? right : left;
    }
}
