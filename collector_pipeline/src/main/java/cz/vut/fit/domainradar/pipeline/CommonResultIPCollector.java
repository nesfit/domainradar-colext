package cz.vut.fit.domainradar.pipeline;

import cz.vut.fit.domainradar.models.results.CommonIPResult;

import java.time.Instant;

public interface CommonResultIPCollector<TData> extends IPCollector<CommonIPResult<TData>> {
    default CommonIPResult<TData> successResult(TData data) {
        return new CommonIPResult<>(true, null, Instant.now(), getCollectorName(), data);
    }

    default CommonIPResult<TData> errorResult(Throwable e) {
        return errorResult(e.getMessage());
    }

    default CommonIPResult<TData> errorResult(String message) {
        return new CommonIPResult<>(false, message, Instant.now(), getCollectorName(), null);
    }
}
