package cz.vut.fit.domainradar.pipeline;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Named;

public interface PipelineComponent {
    boolean RANDOM_DELAYS = true;

    void addTo(StreamsBuilder builder);

    String getName();

    default Named namedOp(String name) {
        return Named.as(getName() + "_" + name);
    }
}
