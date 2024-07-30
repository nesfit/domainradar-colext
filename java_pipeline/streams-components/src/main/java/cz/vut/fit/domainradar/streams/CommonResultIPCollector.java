package cz.vut.fit.domainradar.streams;

import cz.vut.fit.domainradar.models.ResultCodes;
import cz.vut.fit.domainradar.models.results.CommonIPResult;

import java.time.Instant;

public interface CommonResultIPCollector<TData> extends IPCollector<CommonIPResult<TData>> {
    /**
     * Creates a successful result.
     *
     * @param data The data to include in the result.
     * @return A {@link CommonIPResult} instance with the specified data.
     */
    default CommonIPResult<TData> successResult(TData data) {
        return new CommonIPResult<>(ResultCodes.OK, null, Instant.now(), getCollectorName(), data);
    }

    /**
     * Creates an erroneous result.
     *
     * @param code    The error result code.
     * @param message The error message.
     * @return A {@link CommonIPResult} instance with the specified error code and message.
     */
    default CommonIPResult<TData> errorResult(int code, String message) {
        return new CommonIPResult<>(code, message, Instant.now(), getCollectorName(), null);
    }
}
