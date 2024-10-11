package cz.vut.fit.domainradar;

import cz.vut.fit.domainradar.models.results.DNSResult;
import cz.vut.fit.domainradar.serialization.JsonDeserializer;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.common.serialization.Deserializer;

import java.time.Duration;

public class DomainEntriesProcessFunction extends KeyedProcessFunction<String, KafkaDomainEntry, KafkaDomainAggregate> {

    private transient AggregatingState<KafkaDomainEntry, KafkaDomainAggregate> _domainData;
    private transient ValueState<Long> _entryExpirationTimestamp;
    private transient ValueState<Boolean> _completeStateProduced;

    private transient long _finishedEntryGracePeriodMs;
    private transient long _maxEntryLifetimeMs;

    private transient Deserializer<DNSResult> _dnsResultDeserializer;

    @Override
    public void open(OpenContext openContext) {
        AggregatingStateDescriptor<KafkaDomainEntry, KafkaDomainAggregate, KafkaDomainAggregate> domainDataDescriptor
                = new AggregatingStateDescriptor<>("Per-domain data aggregator",
                new DomainEntriesAggregator(), KafkaDomainAggregate.class);
        var entryExpirationTimestampDescriptor
                = new ValueStateDescriptor<>("Current entry expiration timestamp", Long.class);
        var completeStateDescriptor
                = new ValueStateDescriptor<>("Complete result dispatched flag", Boolean.class);

        _domainData = this.getRuntimeContext().getAggregatingState(domainDataDescriptor);
        _entryExpirationTimestamp = this.getRuntimeContext().getState(entryExpirationTimestampDescriptor);
        _completeStateProduced = this.getRuntimeContext().getState(completeStateDescriptor);

        final var parameters = this.getRuntimeContext().getGlobalJobParameters();
        _finishedEntryGracePeriodMs = Duration.ofMillis(
                Long.parseLong(parameters.getOrDefault(MergerConfig.DN_FINISHED_ENTRY_GRACE_PERIOD_MS_CONFIG,
                        MergerConfig.DN_FINISHED_ENTRY_GRACE_PERIOD_DEFAULT))).toMillis();
        _maxEntryLifetimeMs = Duration.ofMillis(
                Long.parseLong(parameters.getOrDefault(MergerConfig.DN_MAX_ENTRY_LIFETIME_MS_CONFIG,
                        MergerConfig.DN_MAX_ENTRY_LIFETIME_DEFAULT))).toMillis();

        var mapper = Common.makeMapper().build();
        _dnsResultDeserializer = new JsonDeserializer<>(mapper, DNSResult.class);
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

        // If zone, DNS and RDAP-DN data are present, we can check verify TLS presence and populate the IPs
        if (currentState.isMaybeComplete()) {
            currentState = this.getFinalStateIfComplete();
        } else {
            currentState = null;
        }

        // getFinalStateIfComplete returns an aggregate iff it is ready to be produced
        if (currentState != null) {
            collector.collect(currentState);
            _completeStateProduced.update(Boolean.TRUE);
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

        if (_completeStateProduced.value() == Boolean.TRUE) {
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
                if (currentState.getZoneData() != null && currentState.getDNSData() != null) {
                    // Deserialize the DNS data and fill in the IP addresses
                    this.deserializeDNSAndExtractIPs(currentState);
                    out.collect(currentState);
                }
                _domainData.clear();
                _entryExpirationTimestamp.clear();
            }
        }
    }

    /**
     * Deserializes the DNS data present in the input aggregate and populates the aggregate's IP list
     * {@link KafkaDomainAggregate#getIPs()} with the IP addresses in the DNS response.
     *
     * @param currentState The aggregate.
     * @return The deserialized {@link DNSResult}.
     */
    private DNSResult deserializeDNSAndExtractIPs(KafkaDomainAggregate currentState) {
        var rawDnsData = currentState.getDNSData();
        if (rawDnsData == null)
            return null;

        // Deserialize the DNS result
        var dnsResult = _dnsResultDeserializer.deserialize(Topics.OUT_DNS, rawDnsData.getValue());
        if (dnsResult == null)
            return null;

        // Extract the IP addresses
        if (dnsResult.ips() != null) {
            currentState.getIPs().clear();
            for (var ip : dnsResult.ips()) {
                currentState.getIPs().add(ip.ip());
            }
        }

        return dnsResult;
    }

    private KafkaDomainAggregate getFinalStateIfComplete() throws Exception {
        var currentState = _domainData.get();
        if (currentState == null)
            return null;

        // Deserialize the DNS result and populate currentState with IP addresses
        var dnsResult = this.deserializeDNSAndExtractIPs(currentState);
        if (dnsResult == null)
            return null;

        // Check if TLS result is expected and if so, if it is present
        if (currentState.getTLSData() != null)
            // TLS data present, no need to check further
            return currentState;

        var dnsData = dnsResult.dnsData();
        if (dnsData == null)
            // No DNS data -> no TLS expected
            return currentState;

        var a = dnsData.A();
        var aaaa = dnsData.AAAA();
        var cname = dnsData.CNAME();

        // If no IP data is present, TLS is not required
        if ((a == null || a.isEmpty()) && (aaaa == null || aaaa.isEmpty())
                && (cname == null || cname.relatedIPs() == null || cname.relatedIPs().isEmpty())) {
            return currentState;
        }

        // Otherwise we're still waiting for TLS
        return null;
    }

    public static final class DomainEntriesAggregator
            implements AggregateFunction<KafkaDomainEntry, KafkaDomainAggregate, KafkaDomainAggregate> {

        @Override
        public KafkaDomainAggregate createAccumulator() {
            return new KafkaDomainAggregate();
        }

        @Override
        public KafkaDomainAggregate add(KafkaDomainEntry kafkaDomainEntry, KafkaDomainAggregate aggregate) {
            if (aggregate.getDomainName().isEmpty()) {
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
