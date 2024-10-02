package cz.vut.fit.domainradar;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class DomainEntriesProcessFunction extends KeyedProcessFunction<String, KafkaDomainEntry, KafkaDomainAggregate> {

    private transient AggregatingState<KafkaDomainEntry, KafkaDomainAggregate> _domainData;

    @Override
    public void open(OpenContext openContext) {
        AggregatingStateDescriptor<KafkaDomainEntry, KafkaDomainAggregate, KafkaDomainAggregate> descriptor
                = new AggregatingStateDescriptor<>("Per-Domain Data Aggregator",
                new DomainEntriesAggregator(), KafkaDomainAggregate.class);

        // IMP: Do we need per-entry TTL? Probably not.
        // descriptor.enableTimeToLive(ttlConfig);
        _domainData = this.getRuntimeContext().getAggregatingState(descriptor);
    }

    @Override
    public void processElement(KafkaDomainEntry kafkaDomainEntry,
                               KeyedProcessFunction<String, KafkaDomainEntry, KafkaDomainAggregate>.Context context,
                               Collector<KafkaDomainAggregate> collector) throws Exception {


    }

    public static final class DomainEntriesAggregator
            implements AggregateFunction<KafkaDomainEntry, KafkaDomainAggregate, KafkaDomainAggregate> {

        @Override
        public KafkaDomainAggregate createAccumulator() {
            return new KafkaDomainAggregate();
        }

        @Override
        public KafkaDomainAggregate add(KafkaDomainEntry kafkaDomainEntry, KafkaDomainAggregate aggregate) {
            switch (kafkaDomainEntry.getTopic()) {
                case Topics.OUT_DNS -> aggregate.setDnsData(
                        ResultFitnessComparator.getMoreUseful(aggregate.getDnsData(), kafkaDomainEntry));
                case Topics.OUT_RDAP_DN -> aggregate.setRdapDnData(
                        ResultFitnessComparator.getMoreUseful(aggregate.getRdapDnData(), kafkaDomainEntry));
                case Topics.OUT_TLS -> aggregate.setTlsData(
                        ResultFitnessComparator.getMoreUseful(aggregate.getTlsData(), kafkaDomainEntry));
                case Topics.OUT_ZONE -> aggregate.setZoneData(
                        ResultFitnessComparator.getMoreUseful(aggregate.getZoneData(), kafkaDomainEntry));
            }

            return aggregate;
        }

        @Override
        public KafkaDomainAggregate getResult(KafkaDomainAggregate aggregate) {
            return aggregate;
        }

        @Override
        public KafkaDomainAggregate merge(KafkaDomainAggregate left, KafkaDomainAggregate right) {
            left.setDnsData(ResultFitnessComparator.getMoreUseful(left.getDnsData(), right.getDnsData()));
            left.setRdapDnData(ResultFitnessComparator.getMoreUseful(left.getRdapDnData(), right.getRdapDnData()));
            left.setTlsData(ResultFitnessComparator.getMoreUseful(left.getTlsData(), right.getTlsData()));
            left.setZoneData(ResultFitnessComparator.getMoreUseful(left.getZoneData(), right.getZoneData()));
            return left;
        }
    }
}
