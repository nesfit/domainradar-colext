package cz.vut.fit.domainradar.pipeline;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import cz.vut.fit.domainradar.models.dns.DNSData;
import cz.vut.fit.domainradar.models.results.CommonIPResult;
import cz.vut.fit.domainradar.models.results.DNSResult;
import cz.vut.fit.domainradar.models.results.ExtendedDNSResult;
import cz.vut.fit.domainradar.models.results.Result;
import cz.vut.fit.domainradar.models.tls.TLSData;
import cz.vut.fit.domainradar.serialization.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.jetbrains.annotations.Nullable;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public class IPDataMergerComponent implements PipelineComponent {
    public record DataForDomainIP(boolean success,
                                  String error,
                                  Instant lastAttempt,
                                  @Nullable DNSData dnsData,
                                  @Nullable TLSData tlsData,
                                  @Nullable String dn,
                                  @Nullable String ip,
                                  @Nullable Map<String, CommonIPResult<JsonNode>> ipData
    ) implements Result {
        public DataForDomainIP(DNSResult result, String dn, String ip) {
            this(result.success(), result.error(), result.lastAttempt(), result.dnsData(), result.tlsData(), dn, ip,
                    null);
        }

        public DataForDomainIP(DataForDomainIP result, Map<String, CommonIPResult<JsonNode>> ipData) {
            this(result.success, result.error, result.lastAttempt, result.dnsData, result.tlsData, result.dn,
                    result.ip, ipData);
        }
    }

    public static class Wtf {
        @JsonProperty
        public ExtendedDNSResult a;
        @JsonProperty
        public HashMap<String, Map<String, CommonIPResult<JsonNode>>> b = new HashMap<>();
    }

    private static final org.slf4j.Logger Logger = LoggerFactory.getLogger(IPDataMergerComponent.class);
    private final ObjectMapper _jsonMapper;

    public IPDataMergerComponent(ObjectMapper jsonMapper) {
        _jsonMapper = jsonMapper;
    }

    @Override
    public void addTo(StreamsBuilder builder) {
        var commonIpResultOfNodeTypeRef = new TypeReference<CommonIPResult<JsonNode>>() {
        };
        var resultTypeRef = new TypeReference<HashMap<String, CommonIPResult<JsonNode>>>() {
        };

        // ipDataKTable is a table of {IP} -> Map<Collector ID, { success, error, JSON data }>
        var ipDataKTable =
                builder.stream("collected_IP_data", Consumed.with(Serdes.String(), Serdes.String()))
                        .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                        .aggregate(HashMap::new, (ip, partialData, aggregate) ->
                        {
                            try {
                                var val = _jsonMapper.readValue(partialData, commonIpResultOfNodeTypeRef);
                                if (val.success()) {
                                    aggregate.put(val.collector(), val);
                                }
                            } catch (Throwable e) {
                                // todo
                                return aggregate;
                            }

                            return aggregate;
                        }, Materialized.with(Serdes.String(), JsonSerde.of(_jsonMapper, resultTypeRef)));

        var x = builder.stream("processed_DNS", Consumed.with(Serdes.String(), JsonSerde.of(_jsonMapper, DNSResult.class)))
                .flatMap((domain, dns) -> dns.ips().stream().map((ip) -> new KeyValue<>(ip, new DataForDomainIP(dns, domain, ip))).toList());

        // x is a stream of {IP} -> { domain data + single IP value }

        x.foreach((k, v) -> Logger.info("IP: " + k + " -> " + v));

        var y = x.join(ipDataKTable, (ip, ipDomainData, aggregatedIpData) ->
                        new DataForDomainIP(ipDomainData, aggregatedIpData), Joined.with(Serdes.String(),
                        JsonSerde.of(_jsonMapper, DataForDomainIP.class),
                        JsonSerde.of(_jsonMapper, resultTypeRef)))
                .map((ip, data) -> new KeyValue<>(data.dn(), data));

        x.foreach((k, v) -> Logger.info("Domain: " + k + " -> " + v));

        var z = y.groupByKey(Grouped.with(Serdes.String(), JsonSerde.of(_jsonMapper, DataForDomainIP.class)))
                .aggregate(Wtf::new, (domain, data, aggregate) -> {
                    if (aggregate.a == null || aggregate.a.lastAttempt() != data.lastAttempt) {
                        aggregate.a = new ExtendedDNSResult(data.success, data.error, data.lastAttempt, data.dnsData,
                                data.tlsData, null);
                    }

                    aggregate.b.put(data.ip(), data.ipData());
                    return aggregate;
                }, Materialized.with(Serdes.String(), JsonSerde.of(_jsonMapper, Wtf.class)))
                .mapValues((v) -> new ExtendedDNSResult(v.a.success(), v.a.error(), v.a.lastAttempt(), v.a.dnsData(),
                        v.a.tlsData(), v.b),
                        Materialized.with(Serdes.String(), JsonSerde.of(_jsonMapper, ExtendedDNSResult.class)));

        z.toStream().foreach((k, v) -> {
            System.out.println("Domain aggregated: " + k + " -> " + v);
            v.ips().forEach((ip, data) -> System.out.println("  - IP: " + ip + " -> " + data));
        });

        z
                .toStream()
                .to("merged_DNS_IP",
                        Produced.with(Serdes.String(), JsonSerde.of(_jsonMapper, ExtendedDNSResult.class)));
    }
}
