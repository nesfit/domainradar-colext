package cz.vut.fit.domainradar.streams;

import cz.vut.fit.domainradar.models.ResultCodes;
import cz.vut.fit.domainradar.models.results.CommonIPResult;

import java.time.Instant;

public interface CommonResultIPCollector<TData> extends IPCollector<CommonIPResult<TData>> {
    default CommonIPResult<TData> successResult(TData data) {
        return new CommonIPResult<>(ResultCodes.OK, null, Instant.now(), getCollectorName(), data);
    }

    default CommonIPResult<TData> errorResult(String message, int code) {
        return new CommonIPResult<>(code, message, Instant.now(), getCollectorName(), null);
    }
}
