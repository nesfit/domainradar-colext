package cz.vut.fit.domainradar;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Accepts the merged result where everything is represented by byte arrays, deserializes them, maps them
 * to an AllCollectedData object, serializes it into a JSON string and emits it.
 */
public class SerdeMappingFunction implements MapFunction<KafkaMergedResult, Tuple2<String, String>> {
    @Override
    public Tuple2<String, String> map(KafkaMergedResult kafkaMergedResult) throws Exception {
        return null;
    }
}
