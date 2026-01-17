package cz.vut.fit.domainradar.models.results;

import cz.vut.fit.domainradar.models.ResultCodes;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;

/**
 * The base interface for records representing a result of a collecting operation.
 *
 * @author Ondřej Ondryáš
 */
public interface Result {
    /**
     * An integer representing the status of the performed collecting operation.
     * The value is one of the constants defined in <code>{@link ResultCodes}</code>.
     */
    int statusCode();

    /**
     * A human-readable error message. Null iff <code>{@link #statusCode()}</code> is 0.
     */
    @Nullable
    String error();

    /**
     * A timestamp of when the collection operation was finished.
     */
    @NotNull
    Instant lastAttempt();

    /**
     * Returns true if the operation was successful, that is, when {@link #statusCode()} is zero.
     */
    default boolean success() {
        return this.statusCode() == ResultCodes.OK;
    }
}
