package cz.vut.fit.domainradar;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class DomainEntriesProcessFunction extends KeyedProcessFunction<String, KafkaDomainEntry, KafkaDomainAggregate> {

    // IMP: Use a rich aggregation function to maintain the "last update" timestamp inside the aggregate?
    private transient AggregatingState<KafkaDomainEntry, KafkaDomainAggregate> _domainData;
    private transient ValueState<Long> _entryExpirationTimestamp;

    private transient long _finishedEntryGracePeriodMs;
    private transient long _maxEntryLifetimeMs;


    @Override
    public void open(OpenContext openContext) {
        AggregatingStateDescriptor<KafkaDomainEntry, KafkaDomainAggregate, KafkaDomainAggregate> domainDataDescriptor
                = new AggregatingStateDescriptor<>("Per-Domain Data Aggregator",
                new DomainEntriesAggregator(), KafkaDomainAggregate.class);
        ValueStateDescriptor<Long> entryExpirationTimestampDescriptor
                = new ValueStateDescriptor<>("Last Update", Long.class);

        // IMP: Do we need per-entry TTL? Probably not.
        // descriptor.enableTimeToLive(ttlConfig);
        _domainData = this.getRuntimeContext().getAggregatingState(domainDataDescriptor);
        _entryExpirationTimestamp = this.getRuntimeContext().getState(entryExpirationTimestampDescriptor);

        // TODO: configurable
        _finishedEntryGracePeriodMs = Duration.ofSeconds(5).toMillis();
        _maxEntryLifetimeMs = Duration.ofMinutes(10).toMillis();
    }

    @Override
    public void processElement(KafkaDomainEntry kafkaDomainEntry,
                               KeyedProcessFunction<String, KafkaDomainEntry, KafkaDomainAggregate>.Context context,
                               Collector<KafkaDomainAggregate> collector) throws Exception {
        _domainData.add(kafkaDomainEntry);

        var currentState = _domainData.get();
        var expirationTimestamp = _entryExpirationTimestamp.value();
        if (currentState == null)
            return; // Should not happen

        // Delete the pending cleanup timer
        if (expirationTimestamp != null)
            context.timerService().deleteEventTimeTimer(expirationTimestamp);
        if (currentState.isComplete()) {
            // The entry is complete, produce it
            collector.collect(currentState);
            // Schedule cleanup
            context.timerService().registerEventTimeTimer(context.timestamp() + _finishedEntryGracePeriodMs);
        } else {
            // Round the expiration time to seconds
            var nextExpiration = ((context.timestamp() + _maxEntryLifetimeMs) / 1000) * 1000;
            // Schedule unsuccessful entry produce
            _entryExpirationTimestamp.update(nextExpiration);
            context.timerService().registerEventTimeTimer(nextExpiration);
        }
    }

    @Override
    public void onTimer(long timestamp,
                        KeyedProcessFunction<String, KafkaDomainEntry, KafkaDomainAggregate>.OnTimerContext ctx,
                        Collector<KafkaDomainAggregate> out) throws Exception {
        var currentState = _domainData.get();
        if (currentState == null)
            return; // Should not happen

        if (currentState.isComplete()) {
            // The last complete snapshot was produced already, just clean the store for the key
            _domainData.clear();
            _entryExpirationTimestamp.clear();
        } else {
            var currentNextExpiration = _entryExpirationTimestamp.value();
            if (currentNextExpiration == null)
                return; // Should not happen

            // Check if this is the latest timer
            if (timestamp == currentNextExpiration) {
                // The entry is not complete, but it's time to produce it anyway
                // Though, only entries with Zone & DNS data should be produced
                if (currentState.getZoneData() != null && currentState.getDNSData() != null)
                    out.collect(currentState);
                _domainData.clear();
                _entryExpirationTimestamp.clear();
            }
        }
    }

    public static final class DomainEntriesAggregator
            implements AggregateFunction<KafkaDomainEntry, KafkaDomainAggregate, KafkaDomainAggregate> {

        @Override
        public KafkaDomainAggregate createAccumulator() {
            return new KafkaDomainAggregate();
        }

        @Override
        public KafkaDomainAggregate add(KafkaDomainEntry kafkaDomainEntry, KafkaDomainAggregate aggregate) {
            if (aggregate.getDomainName() == null) {
                aggregate.setDomainName(kafkaDomainEntry.getDomainName());
            }

            switch (kafkaDomainEntry.getTopic()) {
                case Topics.OUT_DNS -> aggregate.setDNSData(
                        ResultFitnessComparator.getMoreUseful(aggregate.getDNSData(), kafkaDomainEntry));
                case Topics.OUT_RDAP_DN -> aggregate.setRDAPData(
                        ResultFitnessComparator.getMoreUseful(aggregate.getRDAPData(), kafkaDomainEntry));
                case Topics.OUT_TLS -> aggregate.setTLSData(
                        ResultFitnessComparator.getMoreUseful(aggregate.getTLSData(), kafkaDomainEntry));
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
            left.setDNSData(ResultFitnessComparator.getMoreUseful(left.getDNSData(), right.getDNSData()));
            left.setRDAPData(ResultFitnessComparator.getMoreUseful(left.getRDAPData(), right.getRDAPData()));
            left.setTLSData(ResultFitnessComparator.getMoreUseful(left.getTLSData(), right.getTLSData()));
            left.setZoneData(ResultFitnessComparator.getMoreUseful(left.getZoneData(), right.getZoneData()));
            return left;
        }
    }
}
