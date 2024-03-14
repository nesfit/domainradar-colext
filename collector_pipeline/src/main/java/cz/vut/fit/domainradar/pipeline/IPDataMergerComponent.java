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
        final var commonIpResultOfNodeTypeRef = new TypeReference<CommonIPResult<JsonNode>>() {
        };
        final var commonIpResultSerde = JsonSerde.of(_jsonMapper, commonIpResultOfNodeTypeRef);
        final var hashMapWithIpResultsTypeRef = new TypeReference<ConcurrentHashMap<String, CommonIPResult<JsonNode>>>() {
        };
        final var hashMapWithIpResultsSerde = JsonSerde.of(_jsonMapper, hashMapWithIpResultsTypeRef);
        final var hashMapWithAllIpResultsTypeRef = new TypeReference<Map<String, Map<String, CommonIPResult<JsonNode>>>>() {
        };
        final var hashMapWithAllIpResultsSerde = JsonSerde.of(_jsonMapper, hashMapWithAllIpResultsTypeRef);

        final var ipDataPairSerde = JsonSerde.of(_jsonMapper, IPDataPair.class);
        final var dnsResultSerde = JsonSerde.of(_jsonMapper, DNSResult.class);
        final var extendedDnsResultSerde = JsonSerde.of(_jsonMapper, ExtendedDNSResult.class);

        // These two sub-topologies combines the results from the IP collectors per a domain name.

        // allIPDataForDomain is a table of {Domain Name} -> Map<IP, Map<Collector ID, { success, error, JSON data }>>
        var allIPDataForDomain = builder
                // Input: collected_IP_data: a stream of CommonIPResult objects with various kinds of data.
                // At this point, we don't care what the data is, so we work with it as with an opaque JSON object.
                // The input records are keyed by (Domain Name;IP) pairs.
                .stream("collected_IP_data",
                        Consumed.with(StringPairSerde.build(), commonIpResultSerde))
                // Group by the (DN;IP) pairs - the grouping contains results from several collectors for the same DN-IP.
                .groupByKey(Grouped.with(StringPairSerde.build(), commonIpResultSerde))
                // Aggregate the results from the collectors to a HashMap keyed by the collector name.
                // If there are multiple results from the same collector, the last successful one wins.
                // TODO: Determine whether a concurrent hashmap must really be used.
                // The resulting aggregate is a ConcurrentHashMap<Collector name, IP result>; keyed by the (DN;IP)
                .aggregate(ConcurrentHashMap::new, (dnIpPair, partialData, aggregate) -> {
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
                // The stream now contains several (DN;IP) -> Map entries. Group them by the DN.
                // The result is a grouped stream of DN -> (grouping of) IPDataPair(IP, Map)
                .groupBy((dnIpPair, ipDataMap) ->
                                KeyValue.pair(dnIpPair.domainName(), new IPDataPair(dnIpPair.ip(), ipDataMap)),
                        Grouped.with(Serdes.String(), ipDataPairSerde))
                // Aggregate the group to create a single entry for each DN
                // The resulting entry (aggregate type) is a Map<IP address, Map<Collector, Data>>
                .aggregate(ConcurrentHashMap::new, (dn, ipDataMap, aggregate) -> {
                            // Create the map of maps by putting all IPs from the group to a concurrent hashmap.
                            // I think that newer records should always come last but this should be further looked into
                            // TODO
                            aggregate.put(ipDataMap.ip, ipDataMap.ipData);
                            return aggregate;
                        }, (dn, ipDataMap, aggregate) -> {
                            // The "subtractor" is always called with the old value when a record in the KTable is being
                            // updated or deleted. In our case, it could do nothing as the map replaces the item anyway
                            // and nulls should not propagate here; but let's keep it here for the sake of completeness.
                            aggregate.remove(ipDataMap.ip);
                            return aggregate;
                        }, namedOp("aggregate_IP_data_per_DN"),
                        Materialized.with(Serdes.String(), hashMapWithAllIpResultsSerde));

        // The second topology materializes the "processed DNS" stream as a KTable - this is fine, we only care about
        // the last observed DNS data. Then it joins with the IP data per domain generated above to output a single
        // merged DNS/IP data object.
        var processedDnsTable = builder.table("processed_DNS",
                Consumed.with(Serdes.String(), dnsResultSerde));

        processedDnsTable
                .filter((domain, dns) -> dns != null && dns.ips() != null && !dns.ips().isEmpty(),
                        namedOp("filter_out_DNS_records_without_IPs"))
                .join(allIPDataForDomain, ExtendedDNSResult::new,
                        namedOp("join_aggregated_IP_data_with_DNS_data"),
                        Materialized.with(Serdes.String(), extendedDnsResultSerde))
                .toStream(namedOp("joined_IP_and_DNS_data_to_stream"))
                .to("merged_DNS_IP", Produced.with(Serdes.String(), extendedDnsResultSerde));

        // This topology ensures that DNS records with no IPs are also present in the final collection.
        processedDnsTable
                .filter((domain, dns) -> dns == null || dns.ips() == null || dns.ips().isEmpty())
                .mapValues((v) -> new ExtendedDNSResult(v, null))
                .toStream(namedOp("DNS_with_no_IPs_to_merged"))
                .to("merged_DNS_IP", Produced.with(Serdes.String(), extendedDnsResultSerde));
    }

    @Override
    public String getName() {
        return "DNS_IP_MERGER";
    }
}
