package cz.vut.fit.domainradar;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.internals.SessionWindow;

import java.time.Duration;
import java.util.Properties;


public class DataStreamJob {

    private static final Properties kafkaProperties = new Properties();

    public static void main(String[] args) throws Exception {
        // Set up the Flink execution environment
        final Configuration pipelineConfig = new Configuration();
        pipelineConfig.set(PipelineOptions.FORCE_AVRO, Boolean.TRUE);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(pipelineConfig);

        // Read configuration
        // All configuration properties with the "kafka." prefix will be passed to the Kafka source
        ParameterTool params = ParameterTool.fromPropertiesFile("/home/ondryaso/Projects/domainradar/colext/java_pipeline/client.flink.properties");
        env.getConfig().setGlobalJobParameters(params);
        params.toMap().forEach((k, v) -> {
            if (k.startsWith("kafka.")) {
                kafkaProperties.setProperty(k.substring(6), v);
            }
        });

        KafkaSink<Tuple2<String, String>> sink = makeKafkaSink("all_collected_data");

        var zoneStream = makeKafkaDomainStream(env, "processed_zone");
        var dnsStream = makeKafkaDomainStream(env, "processed_DNS");
        var tlsStream = makeKafkaDomainStream(env, "processed_TLS");
        var rdapDnStream = makeKafkaDomainStream(env, "processed_RDAP_DN");

        var allDomainStreams = zoneStream.union(dnsStream, tlsStream, rdapDnStream)
                .keyBy(KafkaDomainEntry::getDomainName)
                .process(new DomainEntriesProcessFunction())
                .map(dnAggregate -> new KafkaMergedResult(dnAggregate.getDomainName(), dnAggregate))
                .map(new SerdeMappingFunction())
                .sinkTo(sink);


        // Execute the program
        env.execute("DomainRadar Data Merger");
    }

    private static KeyedStream<KafkaDomainEntry, String> makeKafkaDomainStream(final StreamExecutionEnvironment env,
                                                                               final String topic) {
        return env.fromSource(makeKafkaDomainSource(topic), makeWatermarkStrategy(), "Kafka: " + topic)
                .keyBy(KafkaDomainEntry::getDomainName);
    }

    private static KafkaSource<KafkaDomainEntry> makeKafkaDomainSource(final String topic) {
        return KafkaSource.<KafkaDomainEntry>builder()
                .setProperties(kafkaProperties)
                .setTopics(topic)
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setDeserializer(new KafkaDomainEntryDeserializer())
                .build();
    }

    private static KafkaSource<KafkaIPEntry> makeKafkaIpSource(final String topic) {
        return KafkaSource.<KafkaIPEntry>builder()
                .setProperties(kafkaProperties)
                .setTopics(topic)
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setDeserializer(new KafkaIPEntryDeserializer())
                .build();
    }

    private static <K, V> KafkaSink<Tuple2<K, V>> makeKafkaSink(final String topic) {
        return KafkaSink.<Tuple2<K, V>>builder()
                .setKafkaProducerConfig(kafkaProperties)
                .setRecordSerializer(new KafkaSerializer<>(topic))
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }

    private static <T> WatermarkStrategy<T> makeWatermarkStrategy() {
        return WatermarkStrategy
                .<T>forBoundedOutOfOrderness(Duration.ofSeconds(10));
    }
}
