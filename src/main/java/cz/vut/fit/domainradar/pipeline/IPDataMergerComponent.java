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

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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

    @SuppressWarnings("FieldNamingConvention")
    public static class IPDataForDomainAggregator {
        @JsonProperty
        public ExtendedDNSResult dnsData;
        @JsonProperty
        public HashMap<String, List<Map<String, CommonIPResult<JsonNode>>>> ipData = new HashMap<>();
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
        var hashMapWithIpResultsTypeRef = new TypeReference<HashMap<String, CommonIPResult<JsonNode>>>() {
        };
        var hashMapWithIpResultsSerde = JsonSerde.of(_jsonMapper, hashMapWithIpResultsTypeRef);

        var dnsResultSerde = JsonSerde.of(_jsonMapper, DNSResult.class);
        var dataForDomainIpSerde = JsonSerde.of(_jsonMapper, DataForDomainIP.class);
        var extendedDnsResultSerde = JsonSerde.of(_jsonMapper, ExtendedDNSResult.class);

        builder.stream("collected_IP_data", Consumed.with(Serdes.String(), commonIpResultSerde))
                .foreach((k, v) -> Logger.warn("Collected IP data from {}: {}: {}", v.collector(), k, v));

        // This sub-topology combines the results from the IP collectors per IP.
        // aggregatedDataPerIP is a table of {IP} -> Map<Collector ID, { success, error, JSON data }>
        var aggregatedDataPerIP = builder
                // Input: collected_IP_data: a stream of CommonIPResult objects with various kinds of data.
                // At this point, we don't care what the data is, so we work with it as with an opaque JSON object.
                .stream("collected_IP_data", Consumed.with(Serdes.String(), commonIpResultSerde))
                // Group by the IP address - the grouping contains results from several collectors for the same IP.
                .groupByKey(Grouped.with(Serdes.String(), commonIpResultSerde))
                // Aggregate the results from the collectors to a HashMap keyed by the collector name.
                // If there are multiple results from the same collector, the last successful one wins.
                // It doesn't matter that several domains may yield the same IP.
                .aggregate(HashMap::new, (ip, partialData, aggregate) ->
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
                }, namedOp("aggregate_data_per_IP"), Materialized.with(Serdes.String(), hashMapWithIpResultsSerde));

        aggregatedDataPerIP
                .toStream()
                .foreach((k, v) -> Logger.warn("Aggregated data for IP {}:\n\t{}", k, v));

        // This sub-topology consumes the processed DNS data and joins it with the aggregated data for each
        // IP discovered in the DNS.
        //noinspection DataFlowIssue : false positives in .stream()
        var x = builder.stream("processed_DNS", Consumed.with(Serdes.String(), dnsResultSerde))
                // Only take the DNS results that contain IPs (the rest are processed separately below).
                .filter((domain, dns) -> dns != null && dns.ips() != null && !dns.ips().isEmpty(), namedOp("filter_DNS_with_IPs"))
                // Transform the DNS result to a series of data records keyed by the IPs, each containing a copy
                // of the DNS result and a reference to the source domain name and the IP.
                .flatMap((domain, dns) -> dns.ips().stream().map((ip) ->
                        new KeyValue<>(ip, new DataForDomainIP(dns, domain, ip))).toList(), namedOp("flat_map_DNS_data_to_IPs"))
                // Join the per-IP DNS data with the aggregated collected data for the same IP.
                .join(aggregatedDataPerIP,
                        (ip, ipDomainData, aggregatedIpData) -> new DataForDomainIP(ipDomainData, aggregatedIpData),
                        Joined.with(Serdes.String(), dataForDomainIpSerde, hashMapWithIpResultsSerde));

        x.foreach((k, v) -> Logger.warn("Joined data for IP {} (of {}): {}", k, v.dn, v));

        // Re-key the data by the domain name.
        var y = x.map((ip, data) -> new KeyValue<>(data.dn(), data), namedOp("rekey_joined_IP_data_by_domain"))
                // At this point, we've got a stream of {Domain Name} -> DNS data + all collected data for a single IP,
                // there are multiple records for each domain name, one for each IP.
                // Group all records by the domain name.
                .groupByKey(Grouped.with(Serdes.String(), dataForDomainIpSerde))
                .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofSeconds(10)))
                .aggregate(IPDataForDomainAggregator::new, (domain, data, aggregate) -> {
                            if (aggregate.dnsData == null ||
                                    (data.success && data.lastAttempt.isAfter(aggregate.dnsData.lastAttempt()))) {
                                aggregate.dnsData = new ExtendedDNSResult(data.success, data.error, data.lastAttempt,
                                        data.dnsData, data.tlsData, null);
                            }

                            var existingListForIps = aggregate.ipData.get(data.ip());
                            //noinspection Java8MapApi
                            if (existingListForIps == null) {
                                aggregate.ipData.put(data.ip(), existingListForIps = new ArrayList<>());
                            }

                            existingListForIps.add(data.ipData);
                            return aggregate;
                        }, (aggKey, leftVal, rightVal) -> {
                            var merged = new IPDataForDomainAggregator();
                            merged.dnsData = leftVal.dnsData;
                            merged.ipData = new HashMap<>(leftVal.ipData);

                            rightVal.ipData.forEach((ip, rightList) -> {
                                var leftList = merged.ipData.get(ip);
                                if (leftList == null) {
                                    merged.ipData.put(ip, rightList);
                                } else {
                                    leftList.addAll(rightList);
                                }
                            });
                            return merged;
                        }, namedOp("aggregate_collected_IP_data_per_domain"),
                        Materialized.with(Serdes.String(), JsonSerde.of(_jsonMapper, IPDataForDomainAggregator.class)))
                        .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()));


                /*
                // Now we need to aggregate so that the DNS data is kept and the IP data maps are collected into
                // a single map of maps keyed by the IP.
                .aggregate(IPDataForDomainAggregator::new, (domain, data, aggregate) -> {
                            // The aggregator is a simple structure that contains the DNS data and a map of IP -> IP data.
                            // We always want to keep the latest DNS data.
                            if (aggregate.dnsData == null ||
                                    (data.success && data.lastAttempt.isAfter(aggregate.dnsData.lastAttempt()))) {
                                aggregate.dnsData = new ExtendedDNSResult(data.success, data.error, data.lastAttempt,
                                        data.dnsData, data.tlsData, null);
                            }

                            var existingListForIps = aggregate.ipData.get(data.ip());
                            //noinspection Java8MapApi
                            if (existingListForIps == null) {
                                aggregate.ipData.put(data.ip(), existingListForIps = new ArrayList<>());
                            }

                            existingListForIps.add(data.ipData);
                            return aggregate;
                        }, namedOp("aggregate_collected_IP_data_per_domain"),
                        Materialized.with(Serdes.String(), JsonSerde.of(_jsonMapper, IPDataForDomainAggregator.class)));
*/

                y.toStream().foreach((k, v) -> Logger.warn("Aggregated data for domain {}: {}", k, v));

                /*
                y.mapValues((v) -> new ExtendedDNSResult(v.dnsData.success(), v.dnsData.error(), v.dnsData.lastAttempt(), v.dnsData.dnsData(),
                                v.dnsData.tlsData(), v.ipData),
                        Materialized.with(Serdes.String(), extendedDnsResultSerde))
                .toStream()
                .to("merged_DNS_IP",
                        Produced.with(Serdes.String(), extendedDnsResultSerde));*/


        /*
        // Also output the records with no IPs.
        builder.stream("processed_DNS", Consumed.with(Serdes.String(), dnsResultSerde))
                .filter((domain, dns) -> dns == null || dns.ips() == null || dns.ips().isEmpty())
                .mapValues((v) -> new ExtendedDNSResult(v, null))
                .to("merged_DNS_IP", Produced.with(Serdes.String(), extendedDnsResultSerde));*/
    }

    @Override
    public String getName() {
        return "COL_DNS_IP_MERGER";
    }
}
