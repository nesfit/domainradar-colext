package cz.vut.fit.domainradar;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonFactoryBuilder;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JSR310Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import cz.vut.fit.domainradar.models.dns.DNSData;
import cz.vut.fit.domainradar.models.ip.GeoIPData;
import cz.vut.fit.domainradar.models.ip.NERDData;
import cz.vut.fit.domainradar.models.ip.RDAPAddressData;
import cz.vut.fit.domainradar.models.ip.RTTData;
import cz.vut.fit.domainradar.models.results.CommonIPResult;
import cz.vut.fit.domainradar.models.results.DNSResult;
import cz.vut.fit.domainradar.models.results.RDAPDomainResult;
import cz.vut.fit.domainradar.models.results.ZoneResult;
import cz.vut.fit.domainradar.models.dns.ZoneInfo;
import cz.vut.fit.domainradar.models.requests.DNSProcessRequest;
import cz.vut.fit.domainradar.models.requests.ZoneProcessRequest;
import cz.vut.fit.domainradar.models.tls.TLSData;
import cz.vut.fit.domainradar.serialization.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.CountDownLatch;

public class Main {
    private static final Logger Logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        Properties ksProperties = new Properties();
        ksProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "domainradar-pipeline");
        ksProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        ksProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        ksProperties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 6);
        ksProperties.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                "org.apache.kafka.streams.errors.LogAndContinueExceptionHandler");


        final StreamsBuilder builder = new StreamsBuilder();
        final Main main = new Main(builder);

        main.rdapDnCol();
        main.zoneCol();
        main.dnsCol();
        main.rdapIpCol();
        main.geoIpCol();
        main.nerdCol();
        main.rttCol("A");
        main.rttCol("B");
        main.ipDataMerger();

        final Topology topology = builder.build(ksProperties);
        Logger.info("Topology: {}", topology.describe());
        //System.exit(0);

        final CountDownLatch latch = new CountDownLatch(1);
        try (KafkaStreams streams = new KafkaStreams(topology, ksProperties)) {
            // attach shutdown handler to catch control-c
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                streams.close();
                latch.countDown();
            }, "streams-shutdown-hook"));

            try {
                streams.start();
                latch.await();
            } catch (Throwable e) {
                Logger.error("Unhandled exception", e);
                System.exit(1);
            }
        }
    }

    private final StreamsBuilder _builder;
    private final ObjectMapper _objectMapper;

    public Main(StreamsBuilder builder) {
        _builder = builder;
        _objectMapper = JsonMapper.builder()
                .addModule(new JavaTimeModule())
                .build();
    }

    public void rdapDnCol() {
        _builder.stream("to_process_rdap", Consumed.with(Serdes.String(), Serdes.Void()))
                .map((domainName, noValue) -> KeyValue.pair(domainName, new RDAPDomainResult(
                        true, null, Instant.now(),
                        "test1", "test2")))
                .to("processed_rdap_dn", Produced.with(Serdes.String(), JsonSerde.of(_objectMapper, RDAPDomainResult.class)));
    }

    public void zoneCol() {
        var stream = _builder.stream("to_process_zone", Consumed.with(Serdes.String(),
                        JsonSerde.of(_objectMapper, ZoneProcessRequest.class)))
                .map((domainName, request) -> KeyValue.pair(domainName, new ZoneResult(
                        true, null, Instant.now(),
                        new ZoneInfo(new DNSData.SOARecord("test1", "test2", "123", 2, 3, 4, 5),
                                domainName, Set.of("primary NS ip"), Set.of("sec NS 1", "sec NS 2"), Set.of("sec NS ip")))));
        stream.to("processed_zone", Produced.with(Serdes.String(), JsonSerde.of(_objectMapper, ZoneResult.class)));

        stream.filter((domainName, zoneResult) -> zoneResult.zone() != null)
                .map((domainName, zoneResult) -> {
                    assert zoneResult.zone() != null;
                    return KeyValue.pair(domainName, new DNSProcessRequest(List.of("A", "AAAA", "MX"), zoneResult.zone()));
                })
                .to("to_process_DNS", Produced.with(Serdes.String(), JsonSerde.of(_objectMapper, DNSProcessRequest.class)));
    }

    public void dnsCol() {
        var inStream = _builder.stream("to_process_DNS",
                        Consumed.with(Serdes.String(), JsonSerde.of(_objectMapper, DNSProcessRequest.class)))
                .filter((domainName, request) -> request != null && request.zoneInfo() != null);

        var resolved = inStream.map((domainName, request) -> {
            assert request.zoneInfo() != null;
            return KeyValue.pair(domainName, new DNSResult(true, null, Instant.now(),
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
                    Set.of("1.2.3.4", "192.168.1.1", "5a::1", "IP_" + domainName)));
        });

        resolved.to("processed_DNS", Produced.with(Serdes.String(), JsonSerde.of(_objectMapper, DNSResult.class)));

        resolved.flatMap((domainName, data) -> {
                    assert data.ips() != null;
                    return data.ips().stream().map(ip -> KeyValue.pair(ip, (Void) null)).toList();
                })
                .to("to_process_IP", Produced.with(Serdes.String(), Serdes.Void()));
    }

    public void rdapIpCol() {
        var resultTypeRef = new TypeReference<CommonIPResult<RDAPAddressData>>() {
        };

        _builder.stream("to_process_IP", Consumed.with(Serdes.String(), Serdes.Void()))
                .map((ip, noValue) -> KeyValue.pair(ip, new CommonIPResult<>(true, null, Instant.now(), "rdap_ip",
                        new RDAPAddressData("foobar"))))
                .to("collected_IP_data", Produced.with(Serdes.String(), JsonSerde.of(_objectMapper, resultTypeRef)));
    }

    public void geoIpCol() {
        var resultTypeRef = new TypeReference<CommonIPResult<GeoIPData>>() {
        };

        _builder.stream("to_process_IP", Consumed.with(Serdes.String(), Serdes.Void()))
                .map((ip, noValue) -> KeyValue.pair(ip, new CommonIPResult<>(true, null, Instant.now(), "geoip",
                        new GeoIPData("foobar", "asn"))))
                .to("collected_IP_data", Produced.with(Serdes.String(), JsonSerde.of(_objectMapper, resultTypeRef)));
    }

    public void nerdCol() {
        var resultTypeRef = new TypeReference<CommonIPResult<NERDData>>() {
        };

        _builder.stream("to_process_IP", Consumed.with(Serdes.String(), Serdes.Void()))
                .map((ip, noValue) -> KeyValue.pair(ip, new CommonIPResult<>(true, null, Instant.now(), "nerd",
                        new NERDData(0.91))))
                .to("collected_IP_data", Produced.with(Serdes.String(), JsonSerde.of(_objectMapper, resultTypeRef)));
    }

    public void rttCol(String id) {
        var resultTypeRef = new TypeReference<CommonIPResult<RTTData>>() {
        };

        _builder.stream("to_process_IP", Consumed.with(Serdes.String(), Serdes.Void()))
                .map((ip, noValue) -> KeyValue.pair(ip, new CommonIPResult<>(true, null, Instant.now(), "rtt_" + id,
                        new RTTData(true, 25, 0, id))))
                .to("collected_IP_data", Produced.with(Serdes.String(), JsonSerde.of(_objectMapper, resultTypeRef)));
    }

    public void ipDataMerger() {
        var commonIpResultOfNodeTypeRef = new TypeReference<CommonIPResult<JsonNode>>() {
        };
        var resultTypeRef = new TypeReference<HashMap<String, CommonIPResult<JsonNode>>>() {
        };

        var ipDataKTable =
                _builder.stream("collected_IP_data", Consumed.with(Serdes.String(), Serdes.String()))
                        .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                        .aggregate(HashMap::new, (ip, partialData, aggregate) ->
                        {
                            try {
                                var val = _objectMapper.readValue(partialData, commonIpResultOfNodeTypeRef);
                                if (val.success()) {
                                    aggregate.put(val.collector(), val);
                                }
                            } catch (Throwable e) {
                                // todo
                                return aggregate;
                            }

                            return aggregate;
                        }, Materialized.with(Serdes.String(), JsonSerde.of(_objectMapper, resultTypeRef)));

        var ipDataStream = ipDataKTable.toStream();
        ipDataStream.foreach((key, value) -> Logger.warn("IP {} data: {}", key, value));
        ipDataStream.to("grouped_IP_data");
    }
}