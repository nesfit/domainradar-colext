package cz.vut.fit.domainradar.streams;

import cz.vut.fit.domainradar.models.results.Result;

import java.time.Instant;

public interface IPCollector<TResult extends Result>
        extends PipelineCollector<TResult> {

    @Override
    default TResult errorResult(String message, int code, Class<?> clz) {
        try {
            final var constructor = clz.getDeclaredConstructors()[0];
            Object[] parValues = new Object[constructor.getParameterCount()];

            parValues[0] = code;
            parValues[1] = message;
            parValues[2] = Instant.now();
            parValues[3] = getCollectorName();

            //noinspection unchecked
            return (TResult) constructor.newInstance(parValues);
        } catch (Exception constructorException) {
            throw new RuntimeException(constructorException);
        }
    }
}