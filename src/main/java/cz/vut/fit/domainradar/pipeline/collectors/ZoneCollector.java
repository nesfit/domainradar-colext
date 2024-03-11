package cz.vut.fit.domainradar.pipeline.collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.vut.fit.domainradar.models.dns.DNSData;
import cz.vut.fit.domainradar.models.dns.ZoneInfo;
import cz.vut.fit.domainradar.models.requests.DNSProcessRequest;
import cz.vut.fit.domainradar.models.requests.ZoneProcessRequest;
import cz.vut.fit.domainradar.models.results.ZoneResult;
import cz.vut.fit.domainradar.pipeline.PipelineComponent;
import cz.vut.fit.domainradar.serialization.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

import java.time.Instant;
import java.util.List;
import java.util.Random;
import java.util.Set;

public class ZoneCollector implements PipelineComponent {
    private final ObjectMapper _jsonMapper;

    public ZoneCollector(ObjectMapper jsonMapper) {
        _jsonMapper = jsonMapper;
    }

    @Override
    public void addTo(StreamsBuilder builder) {
        final var rnd = new Random();

        var stream = builder.stream("to_process_zone", Consumed.with(Serdes.String(),
                        JsonSerde.of(_jsonMapper, ZoneProcessRequest.class)))
                .map((domainName, request) -> {
                            if (RANDOM_DELAYS) {
                                try {
                                    Thread.sleep(rnd.nextInt(1000));
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
                            }
                            return KeyValue.pair(domainName, new ZoneResult(
                                    true, null, Instant.now(),
                                    new ZoneInfo(new DNSData.SOARecord("test1", "test2", "123", 2, 3, 4, 5),
                                            domainName, Set.of("primary NS ip"), Set.of("sec NS 1", "sec NS 2"), Set.of("sec NS ip"))));
                        },
                        namedOp("resolve"));
        stream.to("processed_zone", Produced.with(Serdes.String(), JsonSerde.of(_jsonMapper, ZoneResult.class)));

        stream.filter((domainName, zoneResult) -> zoneResult.zone() != null)
                .map((domainName, zoneResult) -> {
                    assert zoneResult.zone() != null;
                    return KeyValue.pair(domainName, new DNSProcessRequest(List.of("A", "AAAA", "MX"), zoneResult.zone()));
                }, namedOp("process_DNS_from_zone"))
                .to("to_process_DNS", Produced.with(Serdes.String(), JsonSerde.of(_jsonMapper, DNSProcessRequest.class)));
    }

    @Override
    public String getName() {
        return "COL_ZONE";
    }
}
