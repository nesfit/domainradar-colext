package cz.vut.fit.domainradar.streams;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Named;

import java.io.Closeable;
import java.io.IOException;

/**
 * An interface representing a pipeline component based on Kafka Streams.
 */
public interface PipelineComponent extends Closeable {
    /**
     * Creates the component's topology in the provided StreamsBuilder.
     *
     * @param builder The StreamsBuilder to be used.
     */
    void use(StreamsBuilder builder);

    /**
     * Returns the component identifier.
     *
     * @return The component identifier.
     */
    String getName();

    /**
     * Creates a {@link Named} name for a node in the Streams DSL topology derived
     * from the component's identifier.
     *
     * @param name The target name for the operation/node.
     * @return The name object.
     */
    default Named namedOp(String name) {
        return Named.as(getName() + "_" + name);
    }

    /**
     * Closes the component.
     *
     * @throws IOException If an I/O error occurs.
     */
    @Override
    default void close() throws IOException {
    }
}
