package cz.vut.fit.domainradar;

import cz.vut.fit.domainradar.serialization.TagRegistry;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Process function for reputation systems data collected for a domain name.
 *
 * @author Matěj Čech
 */
public class RepSystemDNEntriesProcessFunction
        extends KeyedCoProcessFunction<String, KafkaRepSystemDNEntry, KafkaDomainAggregate, KafkaDomainWithRepSystemAggregate> {
    // Key: collector tag -> Value: DN data
    private transient MapState<Byte, KafkaRepSystemDNEntry> _collectorToDnData;

    private transient ValueState<Integer> _expectedNumberOfEntries;
    private transient ValueState<KafkaDomainAggregate> _domainData;
    private transient ValueState<Long> _entryExpirationTimestamp;
    private transient ValueState<Boolean> _completeStateProduced;

    private transient long _finishedEntryGracePeriodMs;
    private transient long _maxEntryLifetimeAfterDomainDataMs;
    private transient long _maxEntryLifetimeAfterEachDnMs;
    private transient long _maxWaitForDomainDataMs;

    private static final int TOTAL_EXPECTED_RECORDS = TagRegistry.REP_SYSTEM_DN_TAGS.size();

    private static final Logger LOG = LoggerFactory.getLogger(RepSystemDNEntriesProcessFunction.class);

    @Override
    public void open(OpenContext openContext) throws Exception {
        var collectorToDnDataDescriptor = new MapStateDescriptor<>("Serialized DN data",
                TypeInformation.of(Byte.class), TypeInformation.of(KafkaRepSystemDNEntry.class));
        var expectedNumberOfEntriesDescriptor = new ValueStateDescriptor<>("Number of accepted entries", Integer.class);
        var domainDataDescriptor = new ValueStateDescriptor<>("Domain data", KafkaDomainAggregate.class);
        var entryExpirationTimestampDescriptor = new ValueStateDescriptor<>("Current entry expiration timestamp", Long.class);
        var completeStateDescriptor = new ValueStateDescriptor<>("Complete result dispatched flag", Boolean.class);

        _collectorToDnData = this.getRuntimeContext().getMapState(collectorToDnDataDescriptor);
        _expectedNumberOfEntries = this.getRuntimeContext().getState(expectedNumberOfEntriesDescriptor);
        _domainData = this.getRuntimeContext().getState(domainDataDescriptor);
        _entryExpirationTimestamp = this.getRuntimeContext().getState(entryExpirationTimestampDescriptor);
        _completeStateProduced = this.getRuntimeContext().getState(completeStateDescriptor);

        final var parameters = this.getRuntimeContext().getGlobalJobParameters();
        _finishedEntryGracePeriodMs = Duration.ofMillis(
                Long.parseLong(parameters.getOrDefault(MergerConfig.IP_FINISHED_ENTRY_GRACE_PERIOD_MS_CONFIG,
                        MergerConfig.IP_FINISHED_ENTRY_GRACE_PERIOD_DEFAULT))).toMillis();
        _maxEntryLifetimeAfterDomainDataMs = Duration.ofMillis(
                Long.parseLong(parameters.getOrDefault(MergerConfig.DN_REP_MAX_ENTRY_LIFETIME_AFTER_DOMAIN_DATA_MS_CONFIG,
                        MergerConfig.DN_REP_MAX_ENTRY_LIFETIME_AFTER_DOMAIN_DATA_DEFAULT))).toMillis();
        _maxEntryLifetimeAfterEachDnMs = Duration.ofMillis(
                Long.parseLong(parameters.getOrDefault(MergerConfig.DN_REP_MAX_ENTRY_LIFETIME_AFTER_DN_DATA_MS_CONFIG,
                        MergerConfig.DN_REP_MAX_ENTRY_LIFETIME_AFTER_DN_DATA_DEFAULT))).toMillis();
        _maxWaitForDomainDataMs = Duration.ofMillis(
                Long.parseLong(parameters.getOrDefault(MergerConfig.IP_WAIT_FOR_DOMAIN_DATA_MS_CONFIG,
                        MergerConfig.IP_WAIT_FOR_DOMAIN_DATA_MS_DEFAULT))).toMillis();
    }

    /**
     * Prolongs the lifetime of the currently processed entry (key).
     * The method deletes the currently set timer and sets a new one triggered after {@code timerAfter} milliseconds
     * from now. It does nothing if {@code now + timerAfter} is lower than the currently set target time for the timer.
     * It also does nothing if the complete result has already been produced (so the "finished entry grace period" can
     * never be prolonged).
     *
     * @param ctx        The processing context.
     * @param timerAfter The number of milliseconds to wait (from now) until the entry cleanup timer fires.
     */
    private void updateExpirationTimestamp(KeyedCoProcessFunction<String, ?, ?, ?>.Context ctx,
                                           long timerAfter) throws Exception {
        final var key = ctx.getCurrentKey();
        if (_completeStateProduced.value() == Boolean.TRUE) {
            LOG.trace("[{}] Not updating expiration timestamp for a completed entry", key);
            return;
        }

        var expirationTimestamp = _entryExpirationTimestamp.value();
        // Round the (tentative) new expiration time to seconds
        var nextExpiration = ((ctx.timestamp() + timerAfter) / 1000) * 1000;
        // Set the timer only if it hasn't been set already or the new timestamp is higher than the current one
        if (expirationTimestamp == null || expirationTimestamp < nextExpiration) {
            LOG.trace("[{}] Prolonging expiration by {} from {} to {}", key, timerAfter,
                    expirationTimestamp, nextExpiration);

            if (expirationTimestamp != null) {
                ctx.timerService().deleteEventTimeTimer(expirationTimestamp);
            }
            _entryExpirationTimestamp.update(nextExpiration);
            ctx.timerService().registerEventTimeTimer(nextExpiration);
        } else {
            LOG.trace("[{}] Expiration timestamp not prolonged", key);
        }
    }

    @Override
    public void processElement1(KafkaRepSystemDNEntry value,
                                KeyedCoProcessFunction<String, KafkaRepSystemDNEntry, KafkaDomainAggregate, KafkaDomainWithRepSystemAggregate>.Context ctx,
                                Collector<KafkaDomainWithRepSystemAggregate> out) throws Exception {
        final var dn = ctx.getCurrentKey();
        final var collectorTag = value.collectorTag;

        LOG.trace("[{} -> {}] Accepted reputation system result for DN", dn, collectorTag);
        if (_collectorToDnData.contains(collectorTag)) {
            // If we have already seen this Collector, only insert the new value if it's more useful
            var entry = _collectorToDnData.get(collectorTag);
            if (ResultFitnessComparator.isMoreUseful(entry, value)) {
                LOG.trace("[{} -> {}] Updating entry", dn, collectorTag);
                _collectorToDnData.put(collectorTag, value);

                // Prolong the expiration timer to (now + _maxEntryLifetimeAfterEachDnMs)
                this.updateExpirationTimestamp(ctx, _maxEntryLifetimeAfterEachDnMs);
            }
        }
        else {
            // Otherwise insert the new Collector and increase the "seen data objects for DN" counter
            var seenEntriesForDn = _expectedNumberOfEntries.value();
            if (seenEntriesForDn == null)
                seenEntriesForDn = 0;
            var newEntriesForDn = seenEntriesForDn + 1;

            LOG.trace("[{} -> {}] Increasing seen to {}", dn, collectorTag, newEntriesForDn);
            _expectedNumberOfEntries.update(newEntriesForDn);
            _collectorToDnData.put(collectorTag, value);

            // Prolong the expiration timer to (now + _maxEntryLifetimeAfterEachDnMs)
            this.updateExpirationTimestamp(ctx, _maxEntryLifetimeAfterEachDnMs);
        }

        if (_domainData.value() != null) {
            // Attempt processing if the domain data has already been ingested by the process function
            this.processIngestedData(out, ctx);
        } else {
            // Otherwise, extend the timer to (now + _maxWaitForDomainDataMs) instead
            this.updateExpirationTimestamp(ctx, _maxWaitForDomainDataMs);
        }

        LOG.trace("[{} -> {}] Processing DN result done", dn, collectorTag);
    }

    @Override
    public void processElement2(KafkaDomainAggregate value,
                                KeyedCoProcessFunction<String, KafkaRepSystemDNEntry, KafkaDomainAggregate, KafkaDomainWithRepSystemAggregate>.Context ctx,
                                Collector<KafkaDomainWithRepSystemAggregate> out) throws Exception {
        final var key = ctx.getCurrentKey();
        LOG.trace("[{}] Accepted merged domain data", key);

        // Presume that the domain data only comes "once" (per key lifetime)
        final var currentValue = _domainData.value();

        // Update the data object
        LOG.trace("[{}] Updating domain data state", key);
        _domainData.update(value);

        if (currentValue == null && !_collectorToDnData.isEmpty()) {
            _entryExpirationTimestamp.clear();
        }

        // (Then) prolong the expiration timer to (now + _maxEntryLifetimeAfterDomainDataMs)
        this.updateExpirationTimestamp(ctx, _maxEntryLifetimeAfterDomainDataMs);

        // Check if all required data have been collected
        this.processIngestedData(out, ctx);

        LOG.trace("[{}] Processing domain data done", key);
    }

    private void processIngestedData(@NotNull Collector<KafkaDomainWithRepSystemAggregate> out,
                                     @NotNull KeyedCoProcessFunction<String, ?, ?, ?>.Context ctx) throws Exception {
        final var key = ctx.getCurrentKey();
        LOG.trace("[{}] Processing current state", key);

        if (_expectedNumberOfEntries.value() < TOTAL_EXPECTED_RECORDS) {
            LOG.trace("[{}] Not enough collected data object for an DN", key);
            return;
        }

        // Construct the Collector data list
        var entries = new ArrayList<KafkaRepSystemDNEntry>();
        this.collectDNData(entries);

        // Check the list contains all required entries
        var collectedTags = new ArrayList<Byte>();
        for (var entry : entries) {
            collectedTags.add(entry.collectorTag);
        }

        for (var tag : TagRegistry.REP_SYSTEM_DN_TAGS.values()) {
            if (!collectedTags.contains(tag.byteValue())) {
                LOG.trace("[{}] Missing data for {}", key, tag);
                return;
            }
        }

        // If we got here, we have all the data we need
        var data = _domainData.value();
        var result = new KafkaDomainWithRepSystemAggregate(data.getDomainName(), data, entries);

        LOG.trace("[{}] Producing merged result with DN data from reputation systems", key);
        out.collect(result);

        // Only set the grace period timer after the first successfully produced result
        if (_completeStateProduced.value() != Boolean.TRUE) {
            _entryExpirationTimestamp.clear();
            this.updateExpirationTimestamp(ctx, _finishedEntryGracePeriodMs);
            _completeStateProduced.update(Boolean.TRUE);
        }
    }

    private void collectDNData(@NotNull List<KafkaRepSystemDNEntry> entries) throws Exception {
        for (var entry : _collectorToDnData.entries()) {
            entries.add(entry.getValue());
        }
    }

    @Override
    public void onTimer(long timestamp, KeyedCoProcessFunction<String, KafkaRepSystemDNEntry, KafkaDomainAggregate,
            KafkaDomainWithRepSystemAggregate>.OnTimerContext ctx, Collector<KafkaDomainWithRepSystemAggregate> out)
            throws Exception {
        final var key = ctx.getCurrentKey();

        LOG.trace("[{}] Timer triggered at {} (WM: {})", key, timestamp, ctx.timerService().currentWatermark());

        // Check that this is the latest timer
        final var currentNextExpiration = _entryExpirationTimestamp.value();
        if (currentNextExpiration == null || timestamp != currentNextExpiration) {
            LOG.trace("[{}] Previously canceled timer at {} triggered (current next expiration timestamp: {})",
                    key, timestamp, currentNextExpiration);
            return;
        }

        // If we have already collected all the required data, just clean up
        if (_completeStateProduced.value() == Boolean.TRUE) {
            LOG.trace("[{}] Timer triggered after a complete state has been produced", key);
            this.clearState(ctx);
            return;
        }

        // In case we haven't managed to collect all the required data,
        // collect whatever we have and produce that (if we have any domain data)
        final var data = _domainData.value();

        // We don't want to produce data objects without domain data, missing IPs are fine
        // (this might also be a very lately arriving IP data object)
        if (data == null) {
            LOG.trace("[{}] No domain data encountered", key);
            this.clearState(ctx);
            return;
        }

        var entries = new ArrayList<KafkaRepSystemDNEntry>();
        this.collectDNData(entries);

        final var result = new KafkaDomainWithRepSystemAggregate(data.getDomainName(), data, entries);
        LOG.trace("[{}] Producing incomplete result", key);
        out.collect(result);
        this.clearState(ctx);
    }

    private void clearState(KeyedCoProcessFunction<String, ?, ?, ?>.OnTimerContext ctx) throws IOException {
        // Clear all the state
        LOG.trace("[{}] Clearing up the state", ctx.getCurrentKey());

        var lastExpirationTimestamp = _entryExpirationTimestamp.value();
        if (lastExpirationTimestamp != null) {
            ctx.timerService().deleteEventTimeTimer(lastExpirationTimestamp);
        }

        _domainData.clear();
        _collectorToDnData.clear();
        _expectedNumberOfEntries.clear();
        _entryExpirationTimestamp.clear();
        _completeStateProduced.clear();
    }
}