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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

public class IPEntriesProcessFunction extends KeyedCoProcessFunction<String, KafkaDomainAggregate, KafkaIPEntry, KafkaMergedResult> {

    // Key: Tuple of (IP, collector tag) -> Value: IP data
    private transient MapState<Tuple2<String, Byte>, KafkaIPEntry> _ipAndCollectorToIpData;
    private transient MapState<String, Integer> _expectedIpsToNumberOfEntries;
    private transient ValueState<KafkaDomainAggregate> _domainData;
    private transient ValueState<Long> _entryExpirationTimestamp;
    private transient ValueState<Boolean> _completeStateProduced;

    private transient long _finishedEntryGracePeriodMs;
    private transient long _maxEntryLifetimeAfterDomainDataMs;
    private transient long _maxEntryLifetimeAfterEachIpMs;

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
        var domainDataDescriptor = new ValueStateDescriptor<>("Domain Data", KafkaDomainAggregate.class);
        var entryExpirationTimestampDescriptor = new ValueStateDescriptor<>("Entry expiration timestamp", Long.class);
        var completeStateDescriptor = new ValueStateDescriptor<>("Complete result dispatched", Boolean.class);

        _ipAndCollectorToIpData = this.getRuntimeContext().getMapState(ipAndCollectorToIpDataDescriptor);
        _expectedIpsToNumberOfEntries = this.getRuntimeContext().getMapState(expectedIpsToNumberOfEntriesDescriptor);
        _domainData = this.getRuntimeContext().getState(domainDataDescriptor);
        _entryExpirationTimestamp = this.getRuntimeContext().getState(entryExpirationTimestampDescriptor);
        _completeStateProduced = this.getRuntimeContext().getState(completeStateDescriptor);

        // TODO: configurable
        _finishedEntryGracePeriodMs = Duration.ofSeconds(60).toMillis();
        _maxEntryLifetimeAfterDomainDataMs = Duration.ofMinutes(30).toMillis();
        _maxEntryLifetimeAfterEachIpMs = Duration.ofMinutes(1).toMillis();
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
    private void updateExpirationTimestamp(KeyedCoProcessFunction<?, ?, ?, ?>.Context ctx,
                                           long timerAfter) throws Exception {
        if (_completeStateProduced.value() == Boolean.TRUE) {
            LOG.trace("Not updating expiration timestamp for a completed entry");
            return;
        }

        var expirationTimestamp = _entryExpirationTimestamp.value();
        // Round the (tentative) new expiration time to seconds
        var nextExpiration = ((ctx.timestamp() + timerAfter) / 1000) * 1000;
        // Set the timer only if it hasn't been set already or the new timestamp is higher than the current one
        if (expirationTimestamp == null || expirationTimestamp < nextExpiration) {
            LOG.trace("Updating expiration timestamp from {} to {}", expirationTimestamp, nextExpiration);
            if (expirationTimestamp != null)
                ctx.timerService().deleteEventTimeTimer(expirationTimestamp);

            _entryExpirationTimestamp.update(nextExpiration);
            ctx.timerService().registerEventTimeTimer(nextExpiration);
        }
    }

    @Override
    public void processElement1(KafkaDomainAggregate value,
                                KeyedCoProcessFunction<String, KafkaDomainAggregate, KafkaIPEntry, KafkaMergedResult>.Context ctx,
                                Collector<KafkaMergedResult> out) throws Exception {
        LOG.trace("Accepting incoming domain data");
        // Presume that the domain data only comes "once" (per key lifetime)
        // In case it comes multiple times, overwrite the previous value just in case it contains DNS data
        // and the previous did not
        final var currentValue = _domainData.value();
        if (currentValue != null && (currentValue.getDNSData() != null || value.getDNSData() == null))
            return;

        // Update the data object
        LOG.trace("Updating domain data state");
        _domainData.update(value);

        // Load the expected IP addresses
        var deserializer = new CommonDeserializer();
        var expectedIps = deserializer.parseIPs(value.getDNSData().getValue());

        for (var ip : expectedIps) {
            if (!_expectedIpsToNumberOfEntries.contains(ip))
                _expectedIpsToNumberOfEntries.put(ip, 0);
        }

        // Prolong the expiration timer to (now + _maxEntryLifetimeAfterDomainDataMs)
        this.updateExpirationTimestamp(ctx, _maxEntryLifetimeAfterDomainDataMs);

        // Check if all required data have been collected
        this.processIngestedData(out, ctx);
    }

    @Override
    public void processElement2(KafkaIPEntry value,
                                KeyedCoProcessFunction<String, KafkaDomainAggregate, KafkaIPEntry, KafkaMergedResult>.Context ctx,
                                Collector<KafkaMergedResult> out) throws Exception {
        final var ip = value.ip;
        final var collectorTag = value.collectorTag;
        final var key = Tuple2.of(ip, collectorTag);

        LOG.trace("(IP {}\tColID {}): Processing incoming entry", ip, collectorTag);
        if (_ipAndCollectorToIpData.contains(key)) {
            // If we have already seen this IP-Collector pair, only insert the new value if it's more useful
            var ipEntry = _ipAndCollectorToIpData.get(key);
            if (ResultFitnessComparator.isMoreUseful(ipEntry, value)) {
                LOG.trace("(IP {}\tColID {}): Updating previously seen entry", ip, collectorTag);
                _ipAndCollectorToIpData.put(key, value);
                // Prolong the expiration timer to (now + _maxEntryLifetimeAfterEachIpMs)
                this.updateExpirationTimestamp(ctx, _maxEntryLifetimeAfterEachIpMs);
            }
        } else {
            // Otherwise insert the new IP-Collector pair and increase the "seen data objects for IP" counter
            var seenEntriesForIp = _expectedIpsToNumberOfEntries.get(ip);
            if (seenEntriesForIp == null)
                seenEntriesForIp = 0;
            var newEntriesForIp = seenEntriesForIp + 1;

            LOG.trace("(IP {}\tColID {}): Increasing seen to {}", ip, collectorTag, newEntriesForIp);
            _expectedIpsToNumberOfEntries.put(ip, newEntriesForIp);
            _ipAndCollectorToIpData.put(key, value);
            // Prolong the expiration timer to (now + _maxEntryLifetimeAfterEachIpMs)
            this.updateExpirationTimestamp(ctx, _maxEntryLifetimeAfterEachIpMs);
        }

        // Only attempt processing if the domain data has already been ingested by the process function
        if (_domainData.value() != null)
            this.processIngestedData(out, ctx);

        LOG.trace("(IP {}\tColID {}): Done", ip, collectorTag);
    }

    private void processIngestedData(Collector<KafkaMergedResult> out,
                                     KeyedCoProcessFunction<?, ?, ?, ?>.Context ctx) throws Exception {
        LOG.trace("Processing ingested data");
        // There must be at least TOTAL_EXPECTED_RECORDS_PER_IPS entries for each IP before it makes sense
        // to inspect the data further
        var expectedIps = new HashMap<String, Map<Byte, KafkaIPEntry>>();
        for (var gotEntries : _expectedIpsToNumberOfEntries.entries()) {
            if (gotEntries.getValue() < TOTAL_EXPECTED_RECORDS_PER_IPS)
                return;

            expectedIps.put(gotEntries.getKey(), null);
        }

        // Construct the IP -> (Collector -> Data) map
        this.collectIPData(expectedIps);

        // Check the map contains all required entries
        for (var ip : expectedIps.entrySet()) {
            for (var tag : TagRegistry.TAGS.values()) {
                if (!ip.getValue().containsKey(tag.byteValue())) {
                    return;
                }
            }
        }

        // If we got here, we have all the data we need
        var data = _domainData.value();
        var mergedResult = new KafkaMergedResult(data.getDomainName(), data, expectedIps);

        out.collect(mergedResult);
        // Only set the grace period timer after the first successfully produced result
        if (_completeStateProduced.value() != Boolean.TRUE) {
            _entryExpirationTimestamp.clear();
            this.updateExpirationTimestamp(ctx, _finishedEntryGracePeriodMs);
            _completeStateProduced.update(Boolean.TRUE);
        }
    }

    @Override
    public void onTimer(long timestamp, KeyedCoProcessFunction<String, KafkaDomainAggregate, KafkaIPEntry,
            KafkaMergedResult>.OnTimerContext ctx, Collector<KafkaMergedResult> out) throws Exception {
        LOG.trace("Timer triggered at {}", timestamp);
        // Check that this is the latest timer (just in case - the code should always cancel past timers)
        final var currentNextExpiration = _entryExpirationTimestamp.value();
        if (currentNextExpiration == null || timestamp != currentNextExpiration) {
            LOG.warn("Previously canceled timer at {} triggered (current: {})", timestamp, currentNextExpiration);
            return;
        }

        // In case the function hasn't managed to collect all the required data,
        // collect whatever we have and produce that
        if (_completeStateProduced.value() != Boolean.TRUE) {
            LOG.trace("Entry expired before collecting all required data, producing");

            final var data = _domainData.value();
            // We don't want to produce data objects without domain data, missing IPs are fine
            if (data == null)
                return;

            final var expectedIps = new HashMap<String, Map<Byte, KafkaIPEntry>>();
            for (var gotEntries : _expectedIpsToNumberOfEntries.entries()) {
                expectedIps.put(gotEntries.getKey(), new HashMap<>());
            }

            this.collectIPData(expectedIps);
            final var mergedResult = new KafkaMergedResult(data.getDomainName(), data, expectedIps);
            out.collect(mergedResult);
        }

        // Clear all the state
        LOG.trace("Clearing state");
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
    private void collectIPData(Map<String, Map<Byte, KafkaIPEntry>> targetDataMap) throws Exception {
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
