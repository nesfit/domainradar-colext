package cz.vut.fit.domainradar.pipeline;

import org.apache.kafka.streams.StreamsBuilder;

public interface PipelineComponent {
    void addTo(StreamsBuilder builder);
}
