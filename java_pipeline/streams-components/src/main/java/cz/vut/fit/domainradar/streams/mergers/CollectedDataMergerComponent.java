package cz.vut.fit.domainradar.streams.mergers;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import cz.vut.fit.domainradar.Topics;
import cz.vut.fit.domainradar.models.IPToProcess;
import cz.vut.fit.domainradar.models.ResultCodes;
import cz.vut.fit.domainradar.models.results.*;
import cz.vut.fit.domainradar.serialization.JsonSerde;
import cz.vut.fit.domainradar.streams.PipelineComponent;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CollectedDataMergerComponent implements PipelineComponent {
    public record IPDataPair(String ip, Map<String, CommonIPResult<byte[]>> ipData) {
    }

    private static final org.slf4j.Logger Logger = LoggerFactory.getLogger(CollectedDataMergerComponent.class);
    private final ObjectMapper _jsonMapper;

    public CollectedDataMergerComponent(ObjectMapper jsonMapper) {
        _jsonMapper = jsonMapper;
    }

    @Override
    public void use(StreamsBuilder builder) {
        final var commonIpResultOfNodeTypeRef = new TypeReference<CommonIPResult<byte[]>>() {
        };
        final var commonIpResultSerde = JsonSerde.of(_jsonMapper, commonIpResultOfNodeTypeRef);
        final var hashMapWithIpResultsTypeRef = new TypeReference<ConcurrentHashMap<String, CommonIPResult<byte[]>>>() {
        };
        final var hashMapWithIpResultsSerde = JsonSerde.of(_jsonMapper, hashMapWithIpResultsTypeRef);
        final var hashMapWithAllIpResultsTypeRef = new TypeReference<Map<String, Map<String, CommonIPResult<byte[]>>>>() {
        };
        final var hashMapWithAllIpResultsSerde = JsonSerde.of(_jsonMapper, hashMapWithAllIpResultsTypeRef);

        final var ipToProcessSerde = JsonSerde.of(_jsonMapper, IPToProcess.class);
        final var ipDataPairSerde = JsonSerde.of(_jsonMapper, IPDataPair.class);
        final var dnsResultSerde = JsonSerde.of(_jsonMapper, DNSResult.class);
        final var extendedDnsResultSerde = JsonSerde.of(_jsonMapper, ExtendedDNSResult.class);
        final var rdapDnSerde = JsonSerde.of(_jsonMapper, RDAPDomainResult.class);
        final var zoneResultSerde = JsonSerde.of(_jsonMapper, ZoneResult.class);
        final var tlsResultSerde = JsonSerde.of(_jsonMapper, TLSResult.class);
        final var finalResultSerde = JsonSerde.of(_jsonMapper, FinalResult.class);

        // These two sub-topologies combine the results from the IP collectors per a domain name.

        // allIPDataForDomain is a table of {Domain Name} -> Map<IP, Map<Collector ID, { success, error, JSON data }>>
        var allIPDataForDomain = builder
                // Input: collected_IP_data: a stream of CommonIPResult objects with various kinds of data.
                // At this point, we don't care what the data is, so we work with it as with an opaque JSON object.
                // The input records are keyed by (Domain Name;IP) pairs.
                .stream(Topics.OUT_IP,
                        Consumed.with(ipToProcessSerde, commonIpResultSerde))
                // Group by the (DN;IP) pairs - the grouping contains results from several collectors for the same DN-IP.
                .groupByKey(Grouped.with(ipToProcessSerde, commonIpResultSerde))
                // Aggregate the results from the collectors to a HashMap keyed by the collector name.
                // If there are multiple results from the same collector, the last successful one wins.
                // TODO: Determine whether a concurrent hashmap must really be used.
                // The resulting aggregate is a ConcurrentHashMap<Collector name, IP result>; keyed by the (DN;IP)
                .aggregate(ConcurrentHashMap::new, (dnIpPair, partialData, aggregate) -> {
                            var existingRecord = aggregate.get(partialData.collector());
                            if (existingRecord != null) {
                                // Store the latest successful result.
                                if (partialData.statusCode() == ResultCodes.OK
                                        && (existingRecord.lastAttempt().isBefore(partialData.lastAttempt()))) {
                                    aggregate.put(partialData.collector(), partialData);
                                }
                            } else {
                                // No result from this collector found yet.
                                aggregate.put(partialData.collector(), partialData);
                            }

                            return aggregate;
                        }, namedOp("aggregate_data_per_IP"),
                        Materialized.with(ipToProcessSerde, hashMapWithIpResultsSerde))
                // The stream now contains several (DN;IP) -> Map entries. Group them by the DN.
                // The result is a grouped stream of DN -> (grouping of) IPDataPair(IP, Map)
                .groupBy((dnIpPair, ipDataMap) ->
                                KeyValue.pair(dnIpPair.domainName(), new IPDataPair(dnIpPair.ip(), ipDataMap)),
                        Grouped.with(Serdes.String(), ipDataPairSerde))
                // Aggregate the group to create a single entry for each DN
                // The resulting entry (aggregate type) is a Map<IP address, Map<Collector, Data>>
                .aggregate(ConcurrentHashMap::new, (dn, ipDataMap, aggregate) -> {
                            // Create the map of maps by putting all IPs from the group to a concurrent hashmap.
                            // TODO: I think that newer records should always come last but this should be further looked into
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
        var processedDnsTable = builder.table(Topics.OUT_DNS,
                Consumed.with(Serdes.String(), dnsResultSerde),
                Materialized.with(Serdes.String(), dnsResultSerde));
        var mergedDnsIpTable = processedDnsTable
                .leftJoin(allIPDataForDomain, ExtendedDNSResult::new,
                        namedOp("join_aggregated_IP_data_with_DNS_data"),
                        Materialized.with(Serdes.String(), extendedDnsResultSerde));

        // Now we want to join the DNS/IP data with the data from the other DN-based collectors,
        // that is, zone and RDAP-DN.
        var zoneTable = builder.table(Topics.OUT_ZONE,
                Consumed.with(Serdes.String(), zoneResultSerde));
        var rdapDnTable = builder.table(Topics.OUT_RDAP_DN,
                Consumed.with(Serdes.String(), rdapDnSerde));
        var tlsTable = builder.table(Topics.OUT_TLS,
                Consumed.with(Serdes.String(), tlsResultSerde));

        // We suppose that without DNS(+IP) data, the domain is not interesting at all, hence the first inner join here
        // and the left joins with the other tables.
        var finalResultTable = mergedDnsIpTable
                .join(zoneTable, (dns, zone) -> new FinalResult(dns,
                                null, null, zone.zone()),
                        namedOp("join_ZONE"),
                        Materialized.with(Serdes.String(), finalResultSerde))
                .leftJoin(tlsTable, (intermRes, tls) -> new FinalResult(intermRes.dnsResult(), tls,
                                null, intermRes.zone()),
                        namedOp("join_TLS"),
                        Materialized.with(Serdes.String(), finalResultSerde))
                .leftJoin(rdapDnTable, (intermRes, rdap) -> new FinalResult(intermRes.dnsResult(),
                                intermRes.tlsResult(), rdap, intermRes.zone()),
                        namedOp("join_RDAP_DN"),
                        Materialized.with(Serdes.String(), finalResultSerde));

        // Output the final result to the output topic.
        finalResultTable.toStream(namedOp("result_to_stream"))
                .to(Topics.OUT_MERGE_ALL,
                        Produced.with(Serdes.String(), finalResultSerde));
    }

    @Override
    public String getName() {
        return "COLLECTED_MERGER";
    }
}
