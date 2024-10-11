package cz.vut.fit.domainradar;

import cz.vut.fit.domainradar.models.ResultCodes;
import org.jetbrains.annotations.Nullable;

public final class ResultFitnessComparator {
    public static boolean isMoreUseful(@Nullable final KafkaEntry previous, @Nullable final KafkaEntry current) {
        if (previous == null)
            return true;
        if (current == null)
            return false;

        final var oldNOK = previous.getStatusCode() != ResultCodes.OK;
        final var newOK = current.getStatusCode() == ResultCodes.OK;
        final var newNewer = previous.getTimestamp() < current.getTimestamp();
        return (oldNOK && newNewer) || (oldNOK && newOK) || (newOK && newNewer);
    }

    public static <T extends KafkaEntry> T getMoreUseful(@Nullable final T left, @Nullable final T right) {
        return isMoreUseful(left, right) ? right : left;
    }
}
