package cz.vut.fit.domainradar.pipeline;

import cz.vut.fit.domainradar.models.results.Result;

import java.time.Instant;

public interface IPCollector<TResult extends Result>
        extends PipelineCollector<TResult> {

    @Override
    default TResult errorResult(Throwable e, Class<?> clz) {
        return this.errorResult(e.getMessage(), clz);
    }

    @Override
    default TResult errorResult(String message, Class<?> clz) {
        try {
            final var constructor = clz.getDeclaredConstructors()[0];
            Object[] parValues = new Object[constructor.getParameterCount()];

            parValues[0] = false;
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