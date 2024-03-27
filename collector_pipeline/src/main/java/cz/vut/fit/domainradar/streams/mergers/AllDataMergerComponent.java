package cz.vut.fit.domainradar.streams.mergers;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.vut.fit.domainradar.Topics;
import cz.vut.fit.domainradar.models.results.ExtendedDNSResult;
import cz.vut.fit.domainradar.models.results.FinalResult;
import cz.vut.fit.domainradar.models.results.RDAPDomainResult;
import cz.vut.fit.domainradar.models.results.ZoneResult;
import cz.vut.fit.domainradar.serialization.JsonSerde;
import cz.vut.fit.domainradar.streams.PipelineComponent;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

public class AllDataMergerComponent implements PipelineComponent {

    private final ObjectMapper _jsonMapper;

    public AllDataMergerComponent(ObjectMapper jsonMapper) {
        _jsonMapper = jsonMapper;
    }

    @Override
    public void use(StreamsBuilder builder) {
        final var extendedDnsResultSerde = JsonSerde.of(_jsonMapper, ExtendedDNSResult.class);
        final var rdapDnSerde = JsonSerde.of(_jsonMapper, RDAPDomainResult.class);
        final var zoneResultSerde = JsonSerde.of(_jsonMapper, ZoneResult.class);
        final var finalResultSerde = JsonSerde.of(_jsonMapper, FinalResult.class);

        var mergedDnsIpTable = builder
                .table(Topics.OUT_MERGE_DNS_IP,
                        Consumed.with(Serdes.String(), extendedDnsResultSerde));
        var zoneTable = builder
                .table(Topics.OUT_ZONE,
                        Consumed.with(Serdes.String(), zoneResultSerde));
        var rdapDnTable = builder
                .table(Topics.OUT_RDAP_DN,
                        Consumed.with(Serdes.String(), rdapDnSerde));

        // We suppose that without DNS(+IP) data, the domain is not interesting at all, hence the left join.
        mergedDnsIpTable
                .join(zoneTable, (dns, zone) -> new FinalResult(dns, null, zone.zone()),
                        namedOp("join_DNS_IP_ZONE"),
                        Materialized.with(Serdes.String(), finalResultSerde))
                .leftJoin(rdapDnTable, (intermRes, rdap) -> new FinalResult(intermRes.dnsResult(), rdap, intermRes.zone()),
                        namedOp("join_DNS_IP_ZONE_RDAP"),
                        Materialized.with(Serdes.String(), finalResultSerde))

                .toStream(namedOp("result_to_stream"))
                .to(Topics.OUT_MERGE_ALL,
                        Produced.with(Serdes.String(), finalResultSerde));
    }

    @Override
    public String getName() {
        return "ALL_DATA_MERGER";
    }
}
