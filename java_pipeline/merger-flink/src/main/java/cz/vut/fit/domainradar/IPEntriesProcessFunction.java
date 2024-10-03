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
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

public class IPEntriesProcessFunction extends KeyedCoProcessFunction<String, KafkaDomainAggregate, KafkaIPEntry, KafkaMergedResult> {

    // Key: Tuple of (IP, collector tag) -> Value: IP data
    private transient MapState<Tuple2<String, Byte>, KafkaIPEntry> _ipAndCollectorToIpData;
    private transient MapState<String, Integer> _expectedIpsToNumberOfEntries;
    private transient ValueState<KafkaDomainAggregate> _domainData;

    private transient ValueState<Long> _entryExpirationTimestamp;
    private transient long _finishedEntryGracePeriodMs;
    private transient long _maxEntryLifetimeMs;

    // TODO: Make the mappings configurable
    private static final int TOTAL_EXPECTED_RECORDS_PER_IPS = TagRegistry.TAGS.size();

    @Override
    public void open(OpenContext openContext) throws Exception {
        // Prepare the state using descriptors
        var ipAndCollectorToIpDataDescriptor = new MapStateDescriptor<>("IP and Collector to IP Data",
                TupleTypeInfo.<Tuple2<String, Byte>>getBasicTupleTypeInfo(String.class, Byte.class),
                TypeInformation.of(KafkaIPEntry.class));
        var expectedIpsToNumberOfEntriesDescriptor = new MapStateDescriptor<>("Expected IPs to Number of Entries",
                TypeInformation.of(String.class),
                TypeInformation.of(Integer.class));
        var domainDataDescriptor = new ValueStateDescriptor<>("Domain Data", KafkaDomainAggregate.class);

        _ipAndCollectorToIpData = this.getRuntimeContext().getMapState(ipAndCollectorToIpDataDescriptor);
        _expectedIpsToNumberOfEntries = this.getRuntimeContext().getMapState(expectedIpsToNumberOfEntriesDescriptor);
        _domainData = this.getRuntimeContext().getState(domainDataDescriptor);
    }

    @Override
    public void processElement1(KafkaDomainAggregate value,
                                KeyedCoProcessFunction<String, KafkaDomainAggregate, KafkaIPEntry, KafkaMergedResult>.Context ctx,
                                Collector<KafkaMergedResult> out) throws Exception {
        // Presume that the domain data only comes "once"
        // In case it comes multiple times, overwrite the previous value just in case it contains DNS data
        // and the previous did not
        final var currentValue = _domainData.value();
        if (currentValue != null && (currentValue.getDNSData() != null || value.getDNSData() == null))
            return;

        _domainData.update(value);

        var deserializer = new CommonDeserializer();
        var expectedIps = deserializer.parseIPs(value.getDNSData().getValue());

        for (var ip : expectedIps) {
            if (!_expectedIpsToNumberOfEntries.contains(ip))
                _expectedIpsToNumberOfEntries.put(ip, 0);
        }

        this.processIngestedData(out, ctx.timerService());
    }

    @Override
    public void processElement2(KafkaIPEntry value,
                                KeyedCoProcessFunction<String, KafkaDomainAggregate, KafkaIPEntry, KafkaMergedResult>.Context ctx,
                                Collector<KafkaMergedResult> out) throws Exception {

        final var ip = value.ip;
        final var collectorTag = value.collectorTag;
        final var key = Tuple2.of(ip, collectorTag);

        // System.err.println("Processing (" + ip + ", " + collectorTag + ")");

        if (_ipAndCollectorToIpData.contains(key)) {
            var ipEntry = _ipAndCollectorToIpData.get(key);
            if (ipEntry == null || ResultFitnessComparator.isMoreUseful(ipEntry, value)) {
                _ipAndCollectorToIpData.put(key, value);
            }

            // System.err.println("(" + ip + ", " + collectorTag + "): updating previously seen entry");
        } else {
            var seenEntriesForIp = _expectedIpsToNumberOfEntries.get(ip);
            if (seenEntriesForIp == null)
                seenEntriesForIp = 0;

            // System.err.println("(" + ip + ", " + collectorTag + "): increasing seen to " + (seenEntriesForIp + 1));
            _expectedIpsToNumberOfEntries.put(ip, seenEntriesForIp + 1);
            _ipAndCollectorToIpData.put(key, value);
        }

        // Only attempt processing if the domain data was already ingested by the process function
        if (_domainData.value() != null)
            this.processIngestedData(out, ctx.timerService());
    }

    private void processIngestedData(Collector<KafkaMergedResult> out, TimerService timerService) throws Exception {
        // This is a heuristic to determine if we have all the data we need
        // First, there must be at least TOTAL_EXPECTED_RECORDS_PER_IPS entries for each IP before it makes sense
        // to inspect the data further
        var expectedIps = new HashMap<String, Map<Byte, KafkaIPEntry>>();
        for (var gotEntries : _expectedIpsToNumberOfEntries.entries()) {
            if (gotEntries.getValue() < TOTAL_EXPECTED_RECORDS_PER_IPS)
                return;

            expectedIps.put(gotEntries.getKey(), null);
        }

        // Construct the IP -> (Collector -> Data) map
        for (var ipTagEntry : _ipAndCollectorToIpData.entries()) {
            final var ipTagPair = ipTagEntry.getKey();
            final var entry = ipTagEntry.getValue();

            if (expectedIps.containsKey(ipTagPair.f0)) {
                expectedIps.computeIfAbsent(ipTagPair.f0, k -> new HashMap<>());
                var entriesForIp = expectedIps.get(ipTagPair.f0);
                var currentEntryForTag = entriesForIp.get(ipTagPair.f1);
                if (ResultFitnessComparator.isMoreUseful(currentEntryForTag, entry)) {
                    entriesForIp.put(ipTagPair.f1, entry);
                }
            }
        }

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
        _domainData.clear();
        _ipAndCollectorToIpData.clear();
        _expectedIpsToNumberOfEntries.clear();
    }
}
