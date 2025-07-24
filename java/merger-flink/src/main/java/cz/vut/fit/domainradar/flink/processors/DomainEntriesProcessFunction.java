package cz.vut.fit.domainradar.flink.processors;

import cz.vut.fit.domainradar.Common;
import cz.vut.fit.domainradar.MergerConfig;
import cz.vut.fit.domainradar.Topics;
import cz.vut.fit.domainradar.flink.models.ResultFitnessComparator;
import cz.vut.fit.domainradar.flink.models.KafkaDomainAggregate;
import cz.vut.fit.domainradar.flink.models.KafkaDomainEntry;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;

/**
 * Processes incoming {@link KafkaDomainEntry} domain-based collection results stream keyed by domain name,
 * aggregates results from the collectors (zone, DNS, RDAP-DN, TLS) and emits a {@link KafkaDomainAggregate} when
 * all required data is available or when a timeout expires.
 *
 * <p>This function maintains state for each key:
 * <ul>
 * <li>AggregatingState for partial aggregates.
 * <li>ValueState for the next expiration timestamp.
 * <li>ValueState flag indicating if the complete result was already produced.
 * </ul>
 *
 * <p>Timers are registered to enforce maximum entry lifetimes and a grace period after completion.
 * After the grace period passes, all state for a key (domain name) is cleared.
 */
public class DomainEntriesProcessFunction extends KeyedProcessFunction<String, KafkaDomainEntry, KafkaDomainAggregate> {

    private transient AggregatingState<KafkaDomainEntry, KafkaDomainAggregate> _domainData;
    private transient ValueState<Long> _entryExpirationTimestamp;
    private transient ValueState<Boolean> _completeStateProduced;

    private transient long _finishedEntryGracePeriodMs;
    private transient long _maxEntryLifetimeMs;

    private transient Deserializer<DNSResult> _dnsResultDeserializer;

    private static final Logger LOG = LoggerFactory.getLogger(DomainEntriesProcessFunction.class);

    /**
     * Initializes state descriptors and job parameters for timeouts and deserializer instances.
     */
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

    /**
     * Handles each incoming {@link KafkaDomainEntry}, aggregates it into the current state,
     * schedules or cancels timers, and emits a final result when complete data set is available.
     */
    @Override
    public void processElement(KafkaDomainEntry kafkaDomainEntry,
                               KeyedProcessFunction<String, KafkaDomainEntry, KafkaDomainAggregate>.Context ctx,
                               Collector<KafkaDomainAggregate> collector) throws Exception {
        final var key = ctx.getCurrentKey();
        LOG.trace("[{}] Accepted domain collection result", key);

        _domainData.add(kafkaDomainEntry);

        var currentState = _domainData.get();
        var expirationTimestamp = _entryExpirationTimestamp.value();
        if (currentState == null)
            return; // Should not happen

        // Delete the pending cleanup timer
        if (expirationTimestamp != null) {
            LOG.trace("[{}] Clearing expiration timer at {}", key, expirationTimestamp);
            ctx.timerService().deleteEventTimeTimer(expirationTimestamp);
        }

        // If zone, DNS and RDAP-DN data are present, we can check verify TLS presence and populate the IPs
        if (currentState.isMaybeComplete()) {
            currentState = this.getFinalStateIfComplete();
        } else {
            currentState = null;
        }

        // getFinalStateIfComplete returns an aggregate iff it is ready to be produced
        if (currentState != null) {
            LOG.trace("[{}] Producing final result and scheduling cleanup", key);
            collector.collect(currentState);
            _completeStateProduced.update(Boolean.TRUE);
            // Schedule cleanup
            ctx.timerService().registerEventTimeTimer(ctx.timestamp() + _finishedEntryGracePeriodMs);
        } else {
            // Round the expiration time to seconds
            var nextExpiration = ((ctx.timestamp() + _maxEntryLifetimeMs) / 1000) * 1000;
            // Schedule unsuccessful entry produce
            LOG.trace("[{}] Still hasn't collected all required results, scheduling expiration at {}",
                    key, nextExpiration);
            _entryExpirationTimestamp.update(nextExpiration);
            ctx.timerService().registerEventTimeTimer(nextExpiration);
        }
    }

    /**
     * Invoked when an event-time timer fires. Produces incomplete or cleanup results
     * depending on whether the full data was emitted earlier.
     */
    @Override
    public void onTimer(long timestamp,
                        KeyedProcessFunction<String, KafkaDomainEntry, KafkaDomainAggregate>.OnTimerContext ctx,
                        Collector<KafkaDomainAggregate> out) throws Exception {
        final var key = ctx.getCurrentKey();

        LOG.trace("[{}] Timer triggered at {} (WM: {})", key, timestamp, ctx.timerService().currentWatermark());

        var currentState = _domainData.get();
        if (currentState == null) {
            LOG.warn("[{}] Timer triggered with empty state", key);
            return; // Should not happen
        }

        if (_completeStateProduced.value() == Boolean.TRUE) {
            // The last complete snapshot was produced already, just clean the store for the key
            LOG.trace("[{}] Timer triggered after a complete state has been produced", key);
        } else {
            // Check if this is the latest timer
            var currentNextExpiration = _entryExpirationTimestamp.value();
            if (currentNextExpiration == null || timestamp != currentNextExpiration) {
                LOG.trace("[{}] Previously canceled timer at {} triggered (current next expiration timestamp: {})",
                        key, timestamp, currentNextExpiration);
                return;
            }

            // The entry is not complete, but it's time to produce it anyway
            // Though, only entries with Zone & DNS data should be produced
            if (currentState.getZoneData() != null && currentState.getDNSData() != null) {
                // Deserialize the DNS data and fill in the IP addresses
                this.deserializeDNSAndExtractIPs(currentState);
                LOG.trace("[{}] Producing incomplete result", key);
                out.collect(currentState);
            }

        }

        LOG.trace("[{}] Clearing up the state", key);
        _domainData.clear();
        _completeStateProduced.clear();
        this.clearTimer(ctx);
    }

    private void clearTimer(KeyedProcessFunction<?, ?, ?>.OnTimerContext ctx) throws IOException {
        var timerValue = _entryExpirationTimestamp.value();
        if (timerValue != null) {
            ctx.timerService().deleteEventTimeTimer(timerValue);
        }
        _entryExpirationTimestamp.clear();
    }

    /**
     * Deserializes the DNS data present in the input aggregate and populates the aggregate's IP list
     * {@link KafkaDomainAggregate#getDNSIPs()} with the IP addresses in the DNS response.
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
            currentState.getDNSIPs().clear();
            for (var ip : dnsResult.ips()) {
                currentState.getDNSIPs().add(ip.ip());
            }
        }

        return dnsResult;
    }

    /**
     * Returns an aggregate ready for emission if the DNS/TLS requirements are met.
     * The requirements checked in this method are:
     * <ul>
     * <li>DNS result must be present.
     * <li>If the DNS result bears data, a TLS result must be present if there's a possible source IP (i.e. the DNS
     *     data contain IPs from A, AAAA, or CNAME.)
     * </ul>
     * <p>
     * Mind that this method should only be called after {@link KafkaDomainAggregate#isMaybeComplete()} returns true,
     * as this method doesn't check for the presence of the zone and DNS data.
     *
     * @return The complete {@link KafkaDomainAggregate}, or null if still waiting.
     * @throws Exception on polling state.
     */
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

    /**
     * Utility aggregator combining multiple {@link KafkaDomainEntry} into one {@link KafkaDomainAggregate} based
     * on the results' usefulness.
     *
     * @see ResultFitnessComparator
     */
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
