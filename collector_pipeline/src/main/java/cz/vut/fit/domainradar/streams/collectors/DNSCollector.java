package cz.vut.fit.domainradar.streams.collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.vut.fit.domainradar.models.StringPair;
import cz.vut.fit.domainradar.models.dns.DNSData;
import cz.vut.fit.domainradar.models.requests.DNSProcessRequest;
import cz.vut.fit.domainradar.models.results.DNSResult;
import cz.vut.fit.domainradar.models.tls.TLSData;
import cz.vut.fit.domainradar.streams.PipelineComponent;
import cz.vut.fit.domainradar.serialization.JsonSerde;
import cz.vut.fit.domainradar.serialization.StringPairSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

public class DNSCollector implements PipelineComponent {
    private final ObjectMapper _jsonMapper;

    public DNSCollector(ObjectMapper jsonMapper) {
        _jsonMapper = jsonMapper;
    }

    @Override
    public void use(StreamsBuilder builder) {
        final var rnd = new Random();

        var inStream = builder.stream("to_process_DNS",
                        Consumed.with(Serdes.String(), JsonSerde.of(_jsonMapper, DNSProcessRequest.class)))
                .filter((domainName, request) -> request != null && request.zoneInfo() != null, namedOp("filter_null_requests"));

        var resolved = inStream.mapValues((domainName, request) -> {
            assert request.zoneInfo() != null;

            if (RANDOM_DELAYS) {
                try {
                    Thread.sleep(rnd.nextInt(1000));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            return new DNSResult(0, null, Instant.now(),
                    new DNSData(Map.of("A", 1000, "AAAA", 3600, "MX", 11820),
                            new DNSData.SOARecord("test1", "test2", "123", 2, 3, 4, 5),
                            new DNSData.NSRecord("ns", null),
                            List.of("1.2.3.4", "192.168.1.1"),
                            List.of("5a::1"),
                            null, null, null
                    ),
                    new TLSData("blah", "blahblah", 1,
                            List.of(new TLSData.Certificate("abc", "def", true,
                                    "blah", 1, Instant.MAX, Instant.MIN, 0, null))),
                    Set.of("IP1_" + domainName, "5a::1", "IP2_" + domainName));
        }, namedOp("resolve"));

        resolved.to("processed_DNS", Produced.with(Serdes.String(), JsonSerde.of(_jsonMapper, DNSResult.class)));

        resolved.flatMap((domainName, data) -> {
                    assert data.ips() != null;
                    return data.ips().stream().map(ip -> KeyValue.pair(new StringPair(domainName, ip), (Void) null)).toList();
                }, namedOp("process_IPs_from_DNS"))
                .to("to_process_IP", Produced.with(StringPairSerde.build(), Serdes.Void()));
    }

    @Override
    public String getName() {
        return "COL_DNS";
    }
}