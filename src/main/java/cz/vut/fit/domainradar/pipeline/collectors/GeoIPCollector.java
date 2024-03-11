package cz.vut.fit.domainradar.pipeline.collectors;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import cz.vut.fit.domainradar.models.ip.GeoIPData;
import cz.vut.fit.domainradar.models.results.CommonIPResult;
import cz.vut.fit.domainradar.pipeline.PipelineComponent;
import cz.vut.fit.domainradar.serialization.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

import java.time.Instant;
import java.util.Random;

public class GeoIPCollector implements PipelineComponent {
    private final ObjectMapper _jsonMapper;
    private final TypeReference<CommonIPResult<GeoIPData>> _resultTypeRef = new TypeReference<>() {
    };

    public GeoIPCollector(ObjectMapper jsonMapper) {
        _jsonMapper = jsonMapper;
    }

    @Override
    public void addTo(StreamsBuilder builder) {
        final var rnd = new Random();

        builder.stream("to_process_IP", Consumed.with(Serdes.String(), Serdes.Void()))
                .map((ip, noValue) -> {
                    if (RANDOM_DELAYS) {
                        try {
                            Thread.sleep(rnd.nextInt(500));
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }

                    return KeyValue.pair(ip, new CommonIPResult<>(true, null, Instant.now(),
                            "geoip", new GeoIPData("foobar_" + ip, "AS" + rnd.nextInt())));
                }, namedOp("resolve"))
                .to("collected_IP_data", Produced.with(Serdes.String(), JsonSerde.of(_jsonMapper, _resultTypeRef)));
    }

    @Override
    public String getName() {
        return "COL_GEOIP";
    }
}