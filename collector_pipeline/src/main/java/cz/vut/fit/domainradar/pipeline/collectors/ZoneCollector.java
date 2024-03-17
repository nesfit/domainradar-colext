package cz.vut.fit.domainradar.pipeline.collectors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cz.vut.fit.domainradar.models.dns.DNSData;
import cz.vut.fit.domainradar.models.dns.ZoneInfo;
import cz.vut.fit.domainradar.models.requests.DNSProcessRequest;
import cz.vut.fit.domainradar.models.requests.ZoneProcessRequest;
import cz.vut.fit.domainradar.models.results.ZoneResult;
import cz.vut.fit.domainradar.pipeline.PipelineComponent;
import cz.vut.fit.domainradar.PythonEntryPoint;
import cz.vut.fit.domainradar.serialization.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import py4j.Py4JException;

import java.time.Instant;
import java.util.List;
import java.util.Random;
import java.util.Set;

public class ZoneCollector implements PipelineComponent {
    private final ObjectMapper _jsonMapper;
    private final PythonEntryPoint _pythonEntryPoint;

    public ZoneCollector(ObjectMapper jsonMapper, PythonEntryPoint pythonEntryPoint) {
        _jsonMapper = jsonMapper;
        _pythonEntryPoint = pythonEntryPoint;
    }

    @Override
    public void use(StreamsBuilder builder) {
        var stream = builder
                .stream("to_process_zone",
                        Consumed.with(Serdes.String(), JsonSerde.of(_jsonMapper, ZoneProcessRequest.class)))
                .mapValues((domainName, request) -> {

                    try {
                        var resultJson = _pythonEntryPoint.getZoneInfo(domainName);
                        if (resultJson == null)
                            return new ZoneResult(false, "Not found", Instant.now(), null);

                        var result = _jsonMapper.readValue(resultJson, ZoneInfo.class);
                        return new ZoneResult(true, null, Instant.now(), result);
                    } catch (Exception e) {
                        // TODO
                        return new ZoneResult(false, e.getMessage(), Instant.now(), null);
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
