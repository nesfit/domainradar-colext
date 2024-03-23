package cz.vut.fit.domainradar.streams.collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.vut.fit.domainradar.models.requests.DNSProcessRequest;
import cz.vut.fit.domainradar.models.requests.ZoneProcessRequest;
import cz.vut.fit.domainradar.models.results.ZoneResult;
import cz.vut.fit.domainradar.models.ResultCodes;
import cz.vut.fit.domainradar.streams.PipelineComponent;
import cz.vut.fit.domainradar.serialization.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

import java.time.Instant;
import java.util.List;

public class ZoneCollector implements PipelineComponent {
    private final ObjectMapper _jsonMapper;

    public ZoneCollector(ObjectMapper jsonMapper) {
        _jsonMapper = jsonMapper;
    }

    @Override
    public void use(StreamsBuilder builder) {
        var stream = builder
                .stream("to_process_zone",
                        Consumed.with(Serdes.String(), JsonSerde.of(_jsonMapper, ZoneProcessRequest.class)))
                .mapValues((domainName, request) -> {

                    try {
                        // TODO
                        return new ZoneResult(0, null, Instant.now(), null);
                    } catch (Exception e) {
                        // TODO
                        return new ZoneResult(ResultCodes.OTHER_EXTERNAL_ERROR, e.getMessage(), Instant.now(), null);
                    }
                }, namedOp("resolve"));

        stream
                .to("processed_zone",
                        Produced.with(Serdes.String(), JsonSerde.of(_jsonMapper, ZoneResult.class)));

        stream
                .filter((domainName, zoneResult) -> zoneResult.zone() != null)
                .mapValues((domainName, zoneResult) -> {
                    assert zoneResult.zone() != null;
                    return new DNSProcessRequest(List.of("A", "AAAA", "MX"), zoneResult.zone());
                }, namedOp("process_DNS_from_zone"))
                .to("to_process_DNS",
                        Produced.with(Serdes.String(), JsonSerde.of(_jsonMapper, DNSProcessRequest.class)));
    }

    @Override
    public String getName() {
        return "COL_ZONE";
    }
}
