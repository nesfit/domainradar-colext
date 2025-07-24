package cz.vut.fit.domainradar.flink.models;

import cz.vut.fit.domainradar.models.ResultCodes;
import org.jetbrains.annotations.Nullable;

/**
 * A utility class for comparing collection results based on their "usefulness".
 * Usefulness is determined by the collection status code and the timestamp of the two entries.
 */
public final class ResultFitnessComparator {

    /**
     * Determines if the {@code current} collection result is more useful than the {@code previous} one.
     * The {@code current} result is considered more useful if:
     * <ul>
     * <li>The previous result is null (no previous result exists), or
     * <li>the previous result is not OK, and the current result is OK <b>or</b> newer, or
     * <li>the current result is OK and newer than the previous result.
     * </ul>
     *
     * @param previous The previous result, or null if none exists.
     * @param current  The current result, or null if none exists.
     * @return True if the current entry is more useful, false otherwise.
     */
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

    /**
     * Compares two Kafka entries and returns the more useful one.
     *
     * @param left  The first Kafka entry, or null if none exists.
     * @param right The second Kafka entry, or null if none exists.
     * @param <T>   The type of Kafka entry.
     * @return The more useful Kafka entry, or null if both are null.
     */
    public static <T extends KafkaEntry> T getMoreUseful(@Nullable final T left, @Nullable final T right) {
        return isMoreUseful(left, right) ? right : left;
    }
}