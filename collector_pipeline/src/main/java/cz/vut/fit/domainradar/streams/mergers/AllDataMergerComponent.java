package cz.vut.fit.domainradar.streams.mergers;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.vut.fit.domainradar.models.results.ExtendedDNSResult;
import cz.vut.fit.domainradar.models.results.FinalResult;
import cz.vut.fit.domainradar.models.results.RDAPDomainResult;
import cz.vut.fit.domainradar.streams.PipelineComponent;
import cz.vut.fit.domainradar.serialization.JsonSerde;
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
        final var finalResultSerde = JsonSerde.of(_jsonMapper, FinalResult.class);

        var mergedDnsIpTable = builder
                .table("merged_DNS_IP",
                        Consumed.with(Serdes.String(), extendedDnsResultSerde));
        var rdapDnTable = builder
                .table("processed_RDAP_DN",
                        Consumed.with(Serdes.String(), rdapDnSerde));

        // We suppose that without DNS(+IP) data, the domain is not interesting at all, hence the left join.
        mergedDnsIpTable
                .leftJoin(rdapDnTable, FinalResult::new,
                        namedOp("join_DNS_IP_RDAP"),
                        Materialized.with(Serdes.String(), finalResultSerde))
                .toStream(namedOp("result_to_stream"))
                .to("all_collected_data",
                        Produced.with(Serdes.String(), finalResultSerde));
    }

    @Override
    public String getName() {
        return "ALL_DATA_MERGER";
    }
}
