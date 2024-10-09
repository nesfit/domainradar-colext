package cz.vut.fit.domainradar;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;


public class DataStreamJob {

    private static final Properties kafkaProperties = new Properties();

    public static void main(String[] args) throws Exception {
        // Set up the Flink execution environment
        final Configuration pipelineConfig = new Configuration();
        //pipelineConfig.set(PipelineOptions.FORCE_AVRO, Boolean.TRUE);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(pipelineConfig);

        // Read configuration
        // All configuration properties with the "kafka." prefix will be passed to the Kafka source
        ParameterTool params = ParameterTool.fromPropertiesFile(args[0]);
        env.getConfig().setGlobalJobParameters(params);
        params.toMap().forEach((k, v) -> {
            if (k.startsWith("kafka.")) {
                kafkaProperties.setProperty(k.substring(6), v);
            }
        });

        KafkaSink<Tuple2<String, byte[]>> sink = KafkaSink.<Tuple2<String, byte[]>>builder()
                .setKafkaProducerConfig(kafkaProperties)
                .setRecordSerializer(new KafkaSerializer<>(Topics.OUT_MERGE_ALL, true))
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        var zoneStream = makeKafkaDomainStream(env, Topics.OUT_ZONE);
        var dnsStream = makeKafkaDomainStream(env, Topics.OUT_DNS);
        var tlsStream = makeKafkaDomainStream(env, Topics.OUT_TLS);
        var rdapDnStream = makeKafkaDomainStream(env, Topics.OUT_RDAP_DN);

        var ipDataStream = makeKafkaIpStream(env, Topics.OUT_IP)
                .keyBy(KafkaIPEntry::getDomainName);

        var dnData = zoneStream.union(dnsStream, tlsStream, rdapDnStream)
                .keyBy(KafkaDomainEntry::getDomainName)
                .process(new DomainEntriesProcessFunction())
                .keyBy(KafkaDomainAggregate::getDomainName)
                .connect(ipDataStream)
                .process(new IPEntriesProcessFunction())
                .map(new SerdeMappingFunction());


        var resultWmStrategy = WatermarkStrategy
                .<Tuple2<String, byte[]>>noWatermarks()
                .withTimestampAssigner((event, timestamp) -> Instant.now().toEpochMilli());

        dnData.assignTimestampsAndWatermarks(resultWmStrategy)
                .sinkTo(sink);

        // Execute the program
        env.execute("DomainRadar Data Merger");
    }

    private static KeyedStream<KafkaDomainEntry, String> makeKafkaDomainStream(final StreamExecutionEnvironment env,
                                                                               final String topic) {
        return env.fromSource(makeKafkaDomainSource(topic), makeWatermarkStrategy(), "Kafka: " + topic)
                .keyBy(KafkaDomainEntry::getDomainName);
    }

    private static KeyedStream<KafkaIPEntry, String>
    makeKafkaIpStream(final StreamExecutionEnvironment env, final String topic) {
        return env.fromSource(makeKafkaIpSource(topic), makeWatermarkStrategy(), "Kafka: " + topic)
                .keyBy(KafkaIPEntry::getDomainName);
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

    private static <T> WatermarkStrategy<T> makeWatermarkStrategy() {
        return WatermarkStrategy
                .<T>forMonotonousTimestamps();
    }
}
