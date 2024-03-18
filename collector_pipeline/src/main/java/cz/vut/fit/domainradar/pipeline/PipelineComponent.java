package cz.vut.fit.domainradar.pipeline;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Named;

import java.io.Closeable;
import java.io.IOException;

public interface PipelineComponent extends Closeable {
    boolean RANDOM_DELAYS = true;

    void use(StreamsBuilder builder);

    String getName();

    default Named namedOp(String name) {
        return Named.as(getName() + "_" + name);
    }

    default boolean createsStreamTopology() {
        return true;
    }

    @Override
    default void close() throws IOException {
    }
}
