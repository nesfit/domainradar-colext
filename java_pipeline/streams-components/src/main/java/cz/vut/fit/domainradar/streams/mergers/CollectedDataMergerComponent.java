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
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The data merger component that merges collected data from all the collectors.
 * This class implements the PipelineComponent interface.
 */
public class CollectedDataMergerComponent implements PipelineComponent {
    /**
     * A record that holds an IP address and a map of arbitrary collected data for that IP.
     * Used to materialize the intermediate results of the per-IP data aggregation.
     *
     * @param ip     The IP address.
     * @param ipData A map where the key is the collector name and the value is the collected data.
     */
    public record IPDataPair(String ip, Map<String, CommonIPResult<JsonNode>> ipData) {
    }

    // private static final org.slf4j.Logger Logger = LoggerFactory.getLogger(CollectedDataMergerComponent.class);
    private final ObjectMapper _jsonMapper;

    public CollectedDataMergerComponent(ObjectMapper jsonMapper) {
        _jsonMapper = jsonMapper;
    }

    /**
     * Determines if the current result is "more useful" than the previous result.
     * The "current" result is considered more useful if:
     * <ul>
     *  <li>The "previous" result was not OK and the "current" result is newer.</li>
     *  <li>The "previous" result was not OK and the "current" result is OK.</li>
     *  <li>The "current" result is newer and OK.</li>
     * </ul>
     *
     * @param previous The previous result.
     * @param current  The current result.
     * @return true if the current result is more useful; false otherwise.
     */
    public static boolean isMoreUseful(final Result previous, final Result current) {
        final var oldNOK = previous.statusCode() != ResultCodes.OK;
        final var newOK = current.statusCode() == ResultCodes.OK;
        final var newNewer = previous.lastAttempt().isBefore(current.lastAttempt());
        return (oldNOK && newNewer) || (oldNOK && newOK) || (newOK && newNewer);
    }

    @Override
    public void use(StreamsBuilder builder) {
        // TODO: configurable
        final var ipAggregationInactivityGap = Duration.ofMinutes(30);

        final var commonIpResultOfNodeTypeRef = new TypeReference<CommonIPResult<JsonNode>>() {
        };
        final var commonIpResultSerde = JsonSerde.of(_jsonMapper, commonIpResultOfNodeTypeRef);
        final var hashMapWithIpResultsTypeRef = new TypeReference<ConcurrentHashMap<String, CommonIPResult<JsonNode>>>() {
        };
        final var hashMapWithIpResultsSerde = JsonSerde.of(_jsonMapper, hashMapWithIpResultsTypeRef);
        final var hashMapWithAllIpResultsTypeRef = new TypeReference<Map<String, Map<String, CommonIPResult<JsonNode>>>>() {
        };
        final var hashMapWithAllIpResultsSerde = JsonSerde.of(_jsonMapper, hashMapWithAllIpResultsTypeRef);

        final var ipToProcessSerde = JsonSerde.of(_jsonMapper, IPToProcess.class);
        final var ipDataPairSerde = JsonSerde.of(_jsonMapper, IPDataPair.class);
        final var dnsResultSerde = JsonSerde.of(_jsonMapper, DNSResult.class);
        final var rdapDnSerde = JsonSerde.of(_jsonMapper, RDAPDomainResult.class);
        final var zoneResultSerde = JsonSerde.of(_jsonMapper, ZoneResult.class);
        final var tlsResultSerde = JsonSerde.of(_jsonMapper, TLSResult.class);
        final var finalResultSerde = JsonSerde.of(_jsonMapper, AllCollectedData.class);

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
                // Window the results by inactivity gap and grace period to reduce the amount of data in the state store.
                .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(ipAggregationInactivityGap))
                .emitStrategy(EmitStrategy.onWindowUpdate())
                // Aggregate the results from the collectors to a HashMap keyed by the collector name.
                // If there are multiple results from the same collector, the last successful one wins.
                // TODO: Determine whether a concurrent hashmap must really be used.
                // The resulting aggregate is a ConcurrentHashMap<Collector name, IP result>; keyed by the (DN;IP)
                .aggregate(ConcurrentHashMap::new, (dnIpPair, partialData, aggregate) -> {
                            var existingRecord = aggregate.get(partialData.collector());
                            if (existingRecord != null) {
                                // Store the latest successful result.
                                if (isMoreUseful(existingRecord, partialData)) {
                                    aggregate.put(partialData.collector(), partialData);
                                }
                            } else {
                                // No result from this collector found yet.
                                aggregate.put(partialData.collector(), partialData);
                            }

                            return aggregate;
                        }, (key, aggOne, aggTwo) -> {
                            for (var colName : aggOne.keySet()) {
                                if (aggTwo.containsKey(colName)) {
                                    var one = aggOne.get(colName);
                                    var two = aggTwo.get(colName);

                                    if (isMoreUseful(two, one)) {
                                        aggTwo.put(colName, one);
                                    }
                                } else {
                                    aggTwo.put(colName, aggOne.get(colName));
                                }
                            }

                            return aggTwo;
                        }, namedOp("aggregate_data_per_IP"),
                        Materialized.with(ipToProcessSerde, hashMapWithIpResultsSerde))
                // Filter the results to keep only those that have all the required data.
                .filter((dnIpPair, ipDataMap) -> hasResultFromAllIpCollectors(ipDataMap))
                // The stream now contains several (DN;IP) -> Map entries. Group them by the DN.
                // The result is a grouped stream of DN -> (grouping of) IPDataPair(IP, Map)
                .groupBy((dnIpPair, ipDataMap) ->
                                KeyValue.pair(dnIpPair.key().dn(), new IPDataPair(dnIpPair.key().ip(), ipDataMap)),
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
                        Materialized.with(Serdes.String(), hashMapWithAllIpResultsSerde))
                .filter((dn, map) -> !dn.isEmpty());

        // The second topology materializes the "processed DNS" stream as a KTable - this is fine, we only care about
        // the last observed DNS data. Then it joins with the IP data per domain generated above to output a single
        // merged DNS/IP data object.
        var processedDnsTable = builder.table(Topics.OUT_DNS,
                Consumed.with(Serdes.String(), dnsResultSerde),
                Materialized.with(Serdes.String(), dnsResultSerde));
        var mergedDnsIpTable = processedDnsTable
                .leftJoin(allIPDataForDomain, (dnsResult, ipDataMap) -> new AllCollectedData(null,
                                dnsResult, null, null, ipDataMap),
                        namedOp("join_aggregated_IP_data_with_DNS_data"),
                        Materialized.with(Serdes.String(), finalResultSerde))
                // We don't want to output partial values without responses from the IP collectors.
                // The hasEnoughIpCollectorResults method implements a decision process that says
                // whether we have "enough" data on the IPs to pass the output to the next stage
                // of the pipeline.
                .filter((dn, result) -> hasEnoughIpCollectorResults(result));

        // Now we want to join the DNS/IP data with the data from the other DN-based collectors,
        // that is, zone and RDAP-DN.
        var zoneTable = builder.table(Topics.OUT_ZONE,
                Consumed.with(Serdes.String(), zoneResultSerde));
        var rdapDnTable = builder.table(Topics.OUT_RDAP_DN,
                Consumed.with(Serdes.String(), rdapDnSerde));
        var tlsTable = builder.table(Topics.OUT_TLS,
                Consumed.with(Serdes.String(), tlsResultSerde));

        // We require results from all collectors in order to output a data object to the final result.
        // The exception is the TLS collector in case there are no IP addresses in the DNS data.
        var finalResultTable = mergedDnsIpTable
                .leftJoin(tlsTable, (intermRes, tls) -> new AllCollectedData(intermRes.zone(), intermRes.dnsResult(),
                                tls, intermRes.rdapDomainResult(), intermRes.ipResults()),
                        namedOp("join_TLS"),
                        Materialized.with(Serdes.String(), finalResultSerde))
                .filter((dn, result) -> hasTlsIfRequired(result))
                .join(zoneTable, (intermRes, zone) -> new AllCollectedData(zone.zone(), intermRes.dnsResult(),
                                intermRes.tlsResult(), intermRes.rdapDomainResult(), intermRes.ipResults()),
                        namedOp("join_ZONE"),
                        Materialized.with(Serdes.String(), finalResultSerde))
                .join(rdapDnTable, (intermRes, rdap) -> new AllCollectedData(intermRes.zone(), intermRes.dnsResult(),
                                intermRes.tlsResult(), rdap, intermRes.ipResults()),
                        namedOp("join_RDAP_DN"),
                        Materialized.with(Serdes.String(), finalResultSerde));

        // Output the final result to the output topic.
        finalResultTable.toStream(namedOp("result_to_stream"))
                .to(Topics.OUT_MERGE_ALL,
                        Produced.with(Serdes.String(), finalResultSerde));
    }

    public static boolean hasResultFromAllIpCollectors(Map<String, ?> resultsPerIp) {
        return resultsPerIp.containsKey("geo-asn") && resultsPerIp.containsKey("rdap-ip")
                && resultsPerIp.containsKey("rtt");
    }

    public static boolean hasEnoughIpCollectorResults(AllCollectedData result) {
        // Currently, the used decision boundary is simply whether we have at least one
        // collection result for each IP address passed for processing.
        if (result.dnsResult() == null)
            // Sanity check
            return false;

        var ipsFromDns = result.dnsResult().ips();
        if (ipsFromDns == null || ipsFromDns.isEmpty())
            // No IPs discovered, let the DNS result continue
            return true;

        // At least one IP present
        var ipResults = result.ipResults();
        if (ipResults == null || ipResults.isEmpty())
            return false;

        for (var ip : ipResults.keySet()) {
            var forIp = ipResults.get(ip);
            if (forIp == null || forIp.isEmpty())
                return false;

            if (!hasResultFromAllIpCollectors(forIp))
                return false;
            // add NERD?
        }

        for (var ip : ipsFromDns) {
            // No records collected for a related IP
            if (!result.ipResults().containsKey(ip.ip()))
                return false;
        }

        return true;
    }

    /**
     * Checks if the TLS result is present when it should be present.
     * <p>
     * The TLS result should be present if there is at least one IP address in the
     * DNS result for the domain name.
     *
     * @param result The collected data containing DNS and TLS results.
     * @return true if the TLS result is present or not required; false otherwise.
     */
    public static boolean hasTlsIfRequired(AllCollectedData result) {
        if (result.tlsResult() != null)
            // TLS present, no need to check further
            return true;

        if (result.dnsResult() == null)
            // Sanity check
            return false;

        var dnsData = result.dnsResult().dnsData();
        if (dnsData == null)
            // No DNS data -> no TLS expected
            return true;

        var a = dnsData.A();
        var aaaa = dnsData.AAAA();
        var cname = dnsData.CNAME();

        // If no IP data is present, TLS is not required
        return (a == null || a.isEmpty()) && (aaaa == null || aaaa.isEmpty())
                && (cname == null || cname.relatedIPs() == null || cname.relatedIPs().isEmpty());
    }

    @Override
    public String getName() {
        return "merger";
    }
}
