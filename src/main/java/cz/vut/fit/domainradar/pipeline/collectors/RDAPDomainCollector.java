package cz.vut.fit.domainradar.pipeline.collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.vut.fit.domainradar.models.results.RDAPDomainResult;
import cz.vut.fit.domainradar.pipeline.PipelineComponent;
import cz.vut.fit.domainradar.serialization.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

import java.time.Instant;

public class RDAPDomainCollector implements PipelineComponent {
    private final ObjectMapper _jsonMapper;

    public RDAPDomainCollector(ObjectMapper jsonMapper) {
        _jsonMapper = jsonMapper;
    }

    @Override
    public void addTo(StreamsBuilder builder) {
        builder.stream("to_process_RDAP_DN", Consumed.with(Serdes.String(), Serdes.Void()))
                .map((domainName, noValue) -> KeyValue.pair(domainName, new RDAPDomainResult(
                        true, null, Instant.now(),
                        "test1", "test2")), namedOp("resolve"))
                .to("processed_RDAP_DN", Produced.with(Serdes.String(), JsonSerde.of(_jsonMapper, RDAPDomainResult.class)));
    }

    @Override
    public String getName() {
        return "COL_RDAP_DN";
    }
}
