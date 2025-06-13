package cz.vut.fit.domainradar;

import cz.vut.fit.domainradar.serialization.TagRegistry;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

public class IPEntriesProcessFunction extends KeyedCoProcessFunction<String, KafkaIPEntry, KafkaDomainAggregate, KafkaMergedResult> {

    // Key: Tuple of (IP, collector tag) -> Value: IP data
    private transient MapState<Tuple2<String, Byte>, KafkaIPEntry> _ipAndCollectorToIpData;
    private transient MapState<String, Integer> _expectedIpsToNumberOfEntries;
    private transient ValueState<KafkaDomainAggregate> _domainData;
    private transient ValueState<Long> _entryExpirationTimestamp;
    private transient ValueState<Boolean> _completeStateProduced;

    private transient long _finishedEntryGracePeriodMs;
    private transient long _maxEntryLifetimeAfterDomainDataMs;
    private transient long _maxEntryLifetimeAfterEachIpMs;
    private transient long _maxWaitForDomainDataMs;

    // TODO: Make the mappings configurable
    private static final int TOTAL_EXPECTED_RECORDS_PER_IPS = TagRegistry.TAGS.size();

    private static final Logger LOG = LoggerFactory.getLogger(IPEntriesProcessFunction.class);

    @Override
    public void open(OpenContext openContext) throws Exception {
        // Prepare the state using descriptors
        var ipAndCollectorToIpDataDescriptor = new MapStateDescriptor<>("Serialized IP data per DN-IP pair",
                TupleTypeInfo.<Tuple2<String, Byte>>getBasicTupleTypeInfo(String.class, Byte.class),
                TypeInformation.of(KafkaIPEntry.class));
        var expectedIpsToNumberOfEntriesDescriptor
                = new MapStateDescriptor<>("Number of entries accepted for each expected IP",
                TypeInformation.of(String.class),
                TypeInformation.of(Integer.class));
        var domainDataDescriptor = new ValueStateDescriptor<>("Domain data", KafkaDomainAggregate.class);
        var entryExpirationTimestampDescriptor = new ValueStateDescriptor<>("Current entry expiration timestamp", Long.class);
        var completeStateDescriptor = new ValueStateDescriptor<>("Complete result dispatched flag", Boolean.class);

        _ipAndCollectorToIpData = this.getRuntimeContext().getMapState(ipAndCollectorToIpDataDescriptor);
        _expectedIpsToNumberOfEntries = this.getRuntimeContext().getMapState(expectedIpsToNumberOfEntriesDescriptor);
        _domainData = this.getRuntimeContext().getState(domainDataDescriptor);
        _entryExpirationTimestamp = this.getRuntimeContext().getState(entryExpirationTimestampDescriptor);
        _completeStateProduced = this.getRuntimeContext().getState(completeStateDescriptor);

        final var parameters = this.getRuntimeContext().getGlobalJobParameters();
        _finishedEntryGracePeriodMs = Duration.ofMillis(
                Long.parseLong(parameters.getOrDefault(MergerConfig.IP_FINISHED_ENTRY_GRACE_PERIOD_MS_CONFIG,
                        MergerConfig.IP_FINISHED_ENTRY_GRACE_PERIOD_DEFAULT))).toMillis();
        _maxEntryLifetimeAfterDomainDataMs = Duration.ofMillis(
                Long.parseLong(parameters.getOrDefault(MergerConfig.IP_MAX_ENTRY_LIFETIME_AFTER_DOMAIN_DATA_MS_CONFIG,
                        MergerConfig.IP_MAX_ENTRY_LIFETIME_AFTER_DOMAIN_DATA_DEFAULT))).toMillis();
        _maxEntryLifetimeAfterEachIpMs = Duration.ofMillis(
                Long.parseLong(parameters.getOrDefault(MergerConfig.IP_MAX_ENTRY_LIFETIME_AFTER_IP_DATA_MS_CONFIG,
                        MergerConfig.IP_MAX_ENTRY_LIFETIME_AFTER_IP_DATA_DEFAULT))).toMillis();
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
    public void processElement1(KafkaIPEntry value,
                                KeyedCoProcessFunction<String, KafkaIPEntry, KafkaDomainAggregate, KafkaMergedResult>.Context ctx,
                                Collector<KafkaMergedResult> out) throws Exception {
        final var dn = ctx.getCurrentKey();
        final var ip = value.ip;
        final var collectorTag = value.collectorTag;
        final var key = Tuple2.of(ip, collectorTag);

        var shouldProcess = true;

        LOG.trace("[{}][{} -> {}] Accepted IP collection result", dn, ip, collectorTag);
        if (_ipAndCollectorToIpData.contains(key)) {
            // If we have already seen this IP-Collector pair, only insert the new value if it's more useful
            var ipEntry = _ipAndCollectorToIpData.get(key);
            if (ResultFitnessComparator.isMoreUseful(ipEntry, value)) {
                LOG.trace("[{}][{} -> {}] Updating entry", dn, ip, collectorTag);
                _ipAndCollectorToIpData.put(key, value);

                // Prolong the expiration timer to (now + _maxEntryLifetimeAfterEachIpMs)
                this.updateExpirationTimestamp(ctx, _maxEntryLifetimeAfterEachIpMs);
            } else {
                shouldProcess = false;
            }
        } else {
            // Otherwise insert the new IP-Collector pair and increase the "seen data objects for IP" counter
            var seenEntriesForIp = _expectedIpsToNumberOfEntries.get(ip);
            if (seenEntriesForIp == null)
                seenEntriesForIp = 0;
            var newEntriesForIp = seenEntriesForIp + 1;

            LOG.trace("[{}][{} -> {}] Increasing seen to {}", dn, ip, collectorTag, newEntriesForIp);
            _expectedIpsToNumberOfEntries.put(ip, newEntriesForIp);
            _ipAndCollectorToIpData.put(key, value);

            // Prolong the expiration timer to (now + _maxEntryLifetimeAfterEachIpMs)
            this.updateExpirationTimestamp(ctx, _maxEntryLifetimeAfterEachIpMs);
        }

        if (shouldProcess) {
            if (_domainData.value() != null) {
                // Attempt processing if the domain data has already been ingested by the process function
                this.processIngestedData(out, ctx);
            } else {
                // Otherwise, extend the timer to (now + _maxWaitForDomainDataMs) instead
                this.updateExpirationTimestamp(ctx, _maxWaitForDomainDataMs);
            }
        }

        LOG.trace("[{}][{} -> {}] Processing IP result done", dn, ip, collectorTag);
    }

    @Override
    public void processElement2(KafkaDomainAggregate value,
                                KeyedCoProcessFunction<String, KafkaIPEntry, KafkaDomainAggregate, KafkaMergedResult>.Context ctx,
                                Collector<KafkaMergedResult> out) throws Exception {
        final var key = ctx.getCurrentKey();
        LOG.trace("[{}] Accepted domain data", key);

        // Presume that the domain data only comes "once" (per key lifetime)
        // In case it comes multiple times, overwrite the previous value just in case it contains DNS data
        // and the previous did not
        final var currentValue = _domainData.value();
        if (currentValue != null && (currentValue.getDNSData() != null || value.getDNSData() == null))
            return;

        // Update the data object
        LOG.trace("[{}] Updating domain data state", key);
        _domainData.update(value);

        // Load the expected IP addresses
        for (var ip : value.getDNSIPs()) {
            if (!_expectedIpsToNumberOfEntries.contains(ip))
                _expectedIpsToNumberOfEntries.put(ip, 0);
        }

        // If we've already seen an IP address, assume the "wait for domain data" timer is running and cancel it first
        if (currentValue == null && !_ipAndCollectorToIpData.isEmpty()) {
            _entryExpirationTimestamp.clear();
        }

        // (Then) prolong the expiration timer to (now + _maxEntryLifetimeAfterDomainDataMs)
        this.updateExpirationTimestamp(ctx, _maxEntryLifetimeAfterDomainDataMs);

        // Check if all required data have been collected
        this.processIngestedData(out, ctx);

        LOG.trace("[{}] Processing domain data done", key);
    }

    private void processIngestedData(@NotNull Collector<KafkaMergedResult> out,
                                     @NotNull KeyedCoProcessFunction<String, ?, ?, ?>.Context ctx) throws Exception {
        final var key = ctx.getCurrentKey();
        LOG.trace("[{}] Processing current state", key);

        // There must be at least TOTAL_EXPECTED_RECORDS_PER_IPS entries for each IP before it makes sense
        // to inspect the data further
        var expectedIps = new HashMap<String, Map<Byte, KafkaIPEntry>>();
        for (var gotEntries : _expectedIpsToNumberOfEntries.entries()) {
            if (gotEntries.getValue() < TOTAL_EXPECTED_RECORDS_PER_IPS) {
                LOG.trace("[{}] Not enough collected data objects ({}) for IP {}", key,
                        gotEntries.getValue(), gotEntries.getKey());
                return;
            }

            expectedIps.put(gotEntries.getKey(), null);
        }

        // Construct the IP -> (Collector -> Data) map
        this.collectIPData(expectedIps);

        // Check the map contains all required entries
        for (var ip : expectedIps.entrySet()) {
            for (var tag : TagRegistry.TAGS.values()) {
                if (!ip.getValue().containsKey(tag.byteValue())) {
                    LOG.trace("[{}] Missing data for {}", key, tag);
                    return;
                }
            }
        }

        // If we got here, we have all the data we need
        var data = _domainData.value();
        var mergedResult = new KafkaMergedResult(data.getDomainName(), data, expectedIps);

        LOG.trace("[{}] Producing final result", key);
        out.collect(mergedResult);
        // Only set the grace period timer after the first successfully produced result
        if (_completeStateProduced.value() != Boolean.TRUE) {
            _entryExpirationTimestamp.clear();
            this.updateExpirationTimestamp(ctx, _finishedEntryGracePeriodMs);
            _completeStateProduced.update(Boolean.TRUE);
        }
    }

    @Override
    public void onTimer(long timestamp, KeyedCoProcessFunction<String, KafkaIPEntry, KafkaDomainAggregate,
            KafkaMergedResult>.OnTimerContext ctx, Collector<KafkaMergedResult> out) throws Exception {
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

        final var expectedIps = new HashMap<String, Map<Byte, KafkaIPEntry>>();
        for (var gotEntries : _expectedIpsToNumberOfEntries.entries()) {
            expectedIps.put(gotEntries.getKey(), new HashMap<>());
        }

        this.collectIPData(expectedIps);

        final var mergedResult = new KafkaMergedResult(data.getDomainName(), data, expectedIps);
        LOG.trace("[{}] Producing incomplete result", key);
        out.collect(mergedResult);
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
        _ipAndCollectorToIpData.clear();
        _expectedIpsToNumberOfEntries.clear();
        _entryExpirationTimestamp.clear();
        _completeStateProduced.clear();
    }

    /**
     * Transforms the linear map of (IP-CollectorID) -> Data entries stored in the {@code _ipAndCollectorToIpData}
     * map state store to the target Map of Maps representation.
     *
     * @param targetDataMap The target Map of Maps to store the data in.
     */
    private void collectIPData(@NotNull Map<String, Map<Byte, KafkaIPEntry>> targetDataMap) throws Exception {
        for (var ipTagEntry : _ipAndCollectorToIpData.entries()) {
            final var ipTagPair = ipTagEntry.getKey();
            final var entry = ipTagEntry.getValue();

            var entriesForIp = targetDataMap.computeIfAbsent(ipTagPair.f0, k -> new HashMap<>());
            var currentEntryForTag = entriesForIp.get(ipTagPair.f1);
            // In case the IP already contains a result for the collector (f1), only store the
            // new result if it's more useful
            if (ResultFitnessComparator.isMoreUseful(currentEntryForTag, entry)) {
                entriesForIp.put(ipTagPair.f1, entry);
            }
        }
    }
}
