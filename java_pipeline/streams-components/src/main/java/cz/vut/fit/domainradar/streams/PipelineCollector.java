package cz.vut.fit.domainradar.streams;


import cz.vut.fit.domainradar.models.results.Result;

import java.time.Instant;

/**
 * An interface representing a pipeline collector based on Kafka Streams.
 *
 * @param <TResult> The type of the result produced by the collector.
 */
public interface PipelineCollector<TResult extends Result> extends PipelineComponent {
    /**
     * Returns the collector identifier.
     *
     * @return The collector identifier.
     */
    String getCollectorName();

    /**
     * Creates an erroneous result with the specified message and result code.
     *
     * @param message The error message.
     * @param code    The error code.
     * @param clz     The class type of the result.
     * @return The error result of type TResult.
     */
    default TResult errorResult(String message, int code, Class<?> clz) {
        try {
            final var constructor = clz.getDeclaredConstructors()[0];
            Object[] parValues = new Object[constructor.getParameterCount()];

            parValues[0] = code;
            parValues[1] = message;
            parValues[2] = Instant.now();

            //noinspection unchecked
            return (TResult) constructor.newInstance(parValues);
        } catch (Exception constructorException) {
            throw new RuntimeException(constructorException);
        }
    }
}
