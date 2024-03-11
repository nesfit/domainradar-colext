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
import cz.vut.fit.domainradar.serialization.StringPairSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.jetbrains.annotations.Nullable;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class IPDataMergerComponent implements PipelineComponent {
    public record IPDataPair(String ip, Map<String, CommonIPResult<JsonNode>> ipData) {
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
        var commonIpResultSerde = JsonSerde.of(_jsonMapper, commonIpResultOfNodeTypeRef);
        var hashMapWithIpResultsTypeRef = new TypeReference<ConcurrentHashMap<String, CommonIPResult<JsonNode>>>() {
        };
        var hashMapWithIpResultsSerde = JsonSerde.of(_jsonMapper, hashMapWithIpResultsTypeRef);
        var hashMapWithAllIpResultsTypeRef = new TypeReference<Map<String, Map<String, CommonIPResult<JsonNode>>>>() {
        };
        var hashMapWithAllIpResultsSerde = JsonSerde.of(_jsonMapper, hashMapWithAllIpResultsTypeRef);

        var ipDataPairSerde = JsonSerde.of(_jsonMapper, IPDataPair.class);
        var dnsResultSerde = JsonSerde.of(_jsonMapper, DNSResult.class);
        var extendedDnsResultSerde = JsonSerde.of(_jsonMapper, ExtendedDNSResult.class);

        /*builder.stream("collected_IP_data", Consumed.with(StringPairSerde.build(), commonIpResultSerde))
                .foreach((k, v) -> Logger.warn("Collected IP-DN data from {}: {}: {}", v.collector(), k, v));*/

        // This sub-topology combines the results from the IP collectors per IP.

        // aggregatedDataPerIP is a table of {IP} -> Map<Collector ID, { success, error, JSON data }>
        var aggregatedDataPerIP = builder
                // Input: collected_IP_data: a stream of CommonIPResult objects with various kinds of data.
                // At this point, we don't care what the data is, so we work with it as with an opaque JSON object.
                .stream("collected_IP_data", Consumed.with(StringPairSerde.build(), commonIpResultSerde))
                // Group by the IP address - the grouping contains results from several collectors for the same IP.
                .groupByKey(Grouped.with(StringPairSerde.build(), commonIpResultSerde))
                // Aggregate the results from the collectors to a HashMap keyed by the collector name.
                // If there are multiple results from the same collector, the last successful one wins.
                // It doesn't matter that several domains may yield the same IP.
                .aggregate(ConcurrentHashMap::new, (dnIpPair, partialData, aggregate) ->
                        {
                            var existingRecord = aggregate.get(partialData.collector());
                            if (existingRecord != null) {
                                // Store the latest successful result.
                                if (partialData.success()
                                        && (existingRecord.lastAttempt().isBefore(partialData.lastAttempt()))) {
                                    aggregate.put(partialData.collector(), partialData);
                                }
                            } else {
                                // No result from this collector found yet.
                                aggregate.put(partialData.collector(), partialData);
                            }

                            return aggregate;
                        }, namedOp("aggregate_data_per_IP"),
                        Materialized.with(StringPairSerde.build(), hashMapWithIpResultsSerde))

                .groupBy((dnIpPair, ipDataMap) -> KeyValue.pair(dnIpPair.domainName(), new IPDataPair(dnIpPair.ip(), ipDataMap)),
                        Grouped.with(Serdes.String(), ipDataPairSerde))

                .aggregate(ConcurrentHashMap::new, (dn, ipDataMap, aggregate) -> {
                            aggregate.put(ipDataMap.ip, ipDataMap.ipData);
                            return aggregate;
                        }, (dn, ipDataMap, aggergate) -> {
                            aggergate.remove(ipDataMap.ip);
                            return aggergate;
                        }, namedOp("aggregate_IP_data_per_DN"),
                        Materialized.with(Serdes.String(), hashMapWithAllIpResultsSerde));

        /*aggregatedDataPerIP
                .toStream()
                .foreach((k, v) -> Logger.warn("Aggregated IP data for domain {}: {}", k, v));*/

        builder
                .table("processed_DNS", Consumed.with(Serdes.String(), dnsResultSerde))
                .filter((domain, dns) -> dns != null && dns.ips() != null && !dns.ips().isEmpty(),
                        namedOp("filter_out_DNS_records_without_IPs"))
                .join(aggregatedDataPerIP, ExtendedDNSResult::new,
                        namedOp("join_aggregated_IP_data_with_DNS_data"),
                        Materialized.with(Serdes.String(), extendedDnsResultSerde))
                .toStream(namedOp("joined_IP_and_DNS_data_to_stream"))
                .to("merged_DNS_IP", Produced.with(Serdes.String(), extendedDnsResultSerde));

        // addWithoutIPProcessor(builder, dnsResultSerde, extendedDnsResultSerde);
    }

    private static void addWithoutIPProcessor(StreamsBuilder builder, JsonSerde<DNSResult> dnsResultSerde, JsonSerde<ExtendedDNSResult> extendedDnsResultSerde) {
        // Also output the records with no IPs.
        builder.stream("processed_DNS", Consumed.with(Serdes.String(), dnsResultSerde))
                .filter((domain, dns) -> dns == null || dns.ips() == null || dns.ips().isEmpty())
                .mapValues((v) -> new ExtendedDNSResult(v, null))
                .to("merged_DNS_IP", Produced.with(Serdes.String(), extendedDnsResultSerde));
    }

    @Override
    public String getName() {
        return "COL_DNS_IP_MERGER";
    }
}
