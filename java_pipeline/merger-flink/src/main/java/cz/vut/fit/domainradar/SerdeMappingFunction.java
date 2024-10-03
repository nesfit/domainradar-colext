package cz.vut.fit.domainradar;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import cz.vut.fit.domainradar.models.results.*;
import cz.vut.fit.domainradar.serialization.JsonDeserializer;
import cz.vut.fit.domainradar.serialization.JsonSerializer;
import cz.vut.fit.domainradar.serialization.TagRegistry;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Accepts the merged result where everything is represented by byte arrays, deserializes them, maps them
 * to an AllCollectedData object, serializes it into a JSON string and emits it.
 */
public class SerdeMappingFunction extends RichMapFunction<KafkaMergedResult, Tuple2<String, byte[]>> {

    private transient Deserializer<DNSResult> _dnsResultDeserializer;
    private transient Deserializer<TLSResult> _tlsResultDeserializer;
    private transient Deserializer<RDAPDomainResult> _rdapDnResultDeserializer;
    private transient Deserializer<ZoneResult> _zoneResultDeserializer;
    private transient Serializer<AllCollectedData> _finalResultSerializer;
    private transient Deserializer<CommonIPResult<JsonNode>> _ipResultDeserializer;

    @Override
    public void open(OpenContext openContext) throws Exception {
        final var mapper = Common.makeMapper().build();
        _dnsResultDeserializer = new JsonDeserializer<>(mapper, DNSResult.class);
        _tlsResultDeserializer = new JsonDeserializer<>(mapper, TLSResult.class);
        _rdapDnResultDeserializer = new JsonDeserializer<>(mapper, RDAPDomainResult.class);
        _zoneResultDeserializer = new JsonDeserializer<>(mapper, ZoneResult.class);
        _ipResultDeserializer = new JsonDeserializer<>(mapper, new TypeReference<>() {
        });
        _finalResultSerializer = new JsonSerializer<>(mapper);
    }

    @Override
    public Tuple2<String, byte[]> map(KafkaMergedResult kafkaMergedResult) throws Exception {
        final var domainData = kafkaMergedResult.getDomainData();
        final var dnsResult = _dnsResultDeserializer.deserialize(Topics.OUT_DNS, domainData.getDNSData().getValue());
        final var tlsResult = _tlsResultDeserializer.deserialize(Topics.OUT_TLS, domainData.getTLSData().getValue());
        final var rdapDnResult = _rdapDnResultDeserializer.deserialize(Topics.OUT_RDAP_DN, domainData.getRDAPData().getValue());
        final var zoneResult = _zoneResultDeserializer.deserialize(Topics.OUT_ZONE, domainData.getZoneData().getValue());

        final var ipData = new HashMap<String, Map<String, CommonIPResult<JsonNode>>>();
        for (var rawIpDataEntry : kafkaMergedResult.getIPData().entrySet()) {
            var collectorToData = new HashMap<String, CommonIPResult<JsonNode>>();

            for (var rawCollectorDataEntry : rawIpDataEntry.getValue().entrySet()) {
                var collectorName = TagRegistry.COLLECTOR_NAMES.get(rawCollectorDataEntry.getKey().intValue());
                var deserializedEntryData = _ipResultDeserializer.deserialize(Topics.OUT_IP,
                        rawCollectorDataEntry.getValue().getValue());
                collectorToData.put(collectorName, deserializedEntryData);
            }

            ipData.put(rawIpDataEntry.getKey(), collectorToData);
        }

        final var allCollectedData = new AllCollectedData(
                zoneResult.zone(),
                dnsResult,
                tlsResult,
                rdapDnResult,
                ipData
        );

        return Tuple2.of(domainData.getDomainName(),
                _finalResultSerializer.serialize(Topics.OUT_MERGE_ALL, allCollectedData));
    }
}
