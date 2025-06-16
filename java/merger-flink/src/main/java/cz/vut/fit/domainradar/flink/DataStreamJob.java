package cz.vut.fit.domainradar.flink;

import cz.vut.fit.domainradar.MergerConfig;
import cz.vut.fit.domainradar.Topics;
import cz.vut.fit.domainradar.flink.models.*;
import cz.vut.fit.domainradar.flink.sinks.PostgresCollectorResultSink;
import cz.vut.fit.domainradar.flink.processors.DomainEntriesProcessFunction;
import cz.vut.fit.domainradar.flink.processors.IPEntriesProcessFunction;
import cz.vut.fit.domainradar.flink.processors.SerdeMappingFunction;
import cz.vut.fit.domainradar.flink.serialization.KafkaDomainEntryDeserializer;
import cz.vut.fit.domainradar.flink.serialization.KafkaIPEntryDeserializer;
import cz.vut.fit.domainradar.flink.serialization.KafkaSerializer;
import cz.vut.fit.domainradar.flink.sinks.PostgresQRadarSink;
import cz.vut.fit.domainradar.serialization.TagRegistry;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


public class DataStreamJob {

    private static final Properties kafkaProperties = new Properties();
    private static final Properties appProperties = new Properties();
    private static ParameterTool allProperties;
    private static final Logger LOG = LoggerFactory.getLogger(DataStreamJob.class);

    public static void main(String[] args) throws Exception {
        // ==== Configuration ====
        // All configuration properties with the "kafka." prefix will be passed to the Kafka source
        allProperties = ParameterTool.fromPropertiesFile(args[0]);
        LOG.info("Setting application properties:");
        allProperties.toMap().forEach((k, v) -> {
            if (k.startsWith("kafka.")) {
                kafkaProperties.setProperty(k.substring(6), v);
            } else {
                LOG.info("\t{}: {}", k, v);
                appProperties.setProperty(k, v);
            }
        });

        if (allProperties.getBoolean(MergerConfig.IP_DISABLE_NERD, MergerConfig.IP_DISABLE_NERD_DEFAULT)) {
            var nerdId = TagRegistry.TAGS.get("nerd");
            TagRegistry.TAGS_TO_INCLUDE_IN_MERGED_RESULT.remove(nerdId);
        }

        // ==== Flink execution environment ====
        final Configuration pipelineConfig = new Configuration();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(pipelineConfig);
        env.getConfig().setGlobalJobParameters(allProperties);

        // ==== Checkpointing ====
        env.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE);
        final var checkpointConfig = env.getCheckpointConfig();
        // Retain the last checkpoint both when the job fails and when it is manually cancelled
        checkpointConfig.setExternalizedCheckpointRetention(ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
        // Checkpoints may sometimes take longer than on average, don't congest the system
        // with additional checkpoint runs
        checkpointConfig.setMinPauseBetweenCheckpoints(5000);
        // Checkpoints have to complete within 30 seconds, or are discarded
        checkpointConfig.setCheckpointTimeout(30000);
        // Two consecutive checkpoint failures are tolerated
        checkpointConfig.setTolerableCheckpointFailureNumber(5);
        // Allow only one checkpoint to be in progress at the same time
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        // TODO: Determine if unaligned checkpoints help the pipeline
        //       (especially under heavy load)
        // checkpointConfig.enableUnalignedCheckpoints();

        // Create the pipeline
        makePipeline(env);
        //makeTestPipeline(env);

        // ==== Execution ====
        env.execute("DomainRadar Data Merger");
    }

    private static void makeTestPipeline(StreamExecutionEnvironment env) {
        var zoneStream = makeKafkaDomainStream(env, Topics.OUT_ZONE);
        var dnsStream = makeKafkaDomainStream(env, Topics.OUT_DNS);
        var tlsStream = makeKafkaDomainStream(env, Topics.OUT_TLS);
        var rdapDnStream = makeKafkaDomainStream(env, Topics.OUT_RDAP_DN);
        //var ipDataStream = makeKafkaIpStream(env, Topics.OUT_IP)
        //        .keyBy(KafkaIPEntry::getDomainName);

        zoneStream.union(dnsStream, tlsStream, rdapDnStream)
                .keyBy(KafkaDomainEntry::getDomainName)
                .process(new DomainEntriesProcessFunction())
                .uid("dn-merging-processor")
                .print();
    }

    private static void makePipeline(StreamExecutionEnvironment env) {
        // ==== Sources & Sinks ====
        KafkaSink<Tuple2<String, byte[]>> sink = KafkaSink.<Tuple2<String, byte[]>>builder()
                .setKafkaProducerConfig(kafkaProperties)
                .setRecordSerializer(new KafkaSerializer<>(Topics.OUT_MERGE_ALL, true))
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        Sink<KafkaMergedResult> postgresSink = null;
        Sink<KafkaIPEntry> postgresQRadarSink = null;
        if (allProperties.getBoolean(MergerConfig.DB_ENABLED_CONFIG, MergerConfig.DB_ENABLED_DEFAULT)) {
            var url = allProperties.get(MergerConfig.DB_URL_CONFIG, MergerConfig.DB_URL_DEFAULT);
            var user = allProperties.get(MergerConfig.DB_USERNAME_CONFIG, MergerConfig.DB_USERNAME_DEFAULT);
            var password = allProperties.get(MergerConfig.DB_PASSWORD_CONFIG, MergerConfig.DB_PASSWORD_DEFAULT);

            postgresSink = new PostgresCollectorResultSink(url, user, password,
                    allProperties.getBoolean(MergerConfig.DB_STORE_DATA_CONFIG, MergerConfig.DB_STORE_DATA_DEFAULT));

            if (allProperties.getBoolean(MergerConfig.DB_QRADAR_CONFIG, MergerConfig.DB_QRADAR_DEFAULT)) {
                postgresQRadarSink = new PostgresQRadarSink(url, user, password);
            }
        }

        var zoneStream = makeKafkaDomainStream(env, Topics.OUT_ZONE);
        var dnsStream = makeKafkaDomainStream(env, Topics.OUT_DNS);
        var tlsStream = makeKafkaDomainStream(env, Topics.OUT_TLS);
        var rdapDnStream = makeKafkaDomainStream(env, Topics.OUT_RDAP_DN);
        var ipDataStream = makeKafkaIpStream(env);
        var qradarStream = makeKafkaQRadarStream(env);

        // ==== The pipeline ====
        var dnData = zoneStream.union(dnsStream, tlsStream, rdapDnStream)
                .keyBy(KafkaDomainEntry::getDomainName)
                .process(new DomainEntriesProcessFunction())
                .name("Process domain entries")
                .uid("dn-merging-processor")
                .keyBy(KafkaDomainAggregate::getDomainName);

        // Only use the IP merger for entries that actually carried IPs to process
        var dnDataWithIps = dnData
                .filter(dnAggregate -> !dnAggregate.getDNSIPs().isEmpty())
                .name("Filter DN data with IPs")
                .uid("dn-data-with-ips-filter")
                .keyBy(KafkaDomainAggregate::getDomainName);

        var mergedResults = ipDataStream.connect(dnDataWithIps)
                .process(new IPEntriesProcessFunction())
                .name("Process IP entries")
                .uid("dn-ip-final-merging-processor");

        if (postgresSink != null) {
            // Sink the unified DN-IP metadata to PostgreSQL
            mergedResults
                    .sinkTo(postgresSink)
                    .name("Sink collection results to Postgres")
                    .uid("postgres-sink-with-ips");

            // Create "fake merged results" with only the QRadar entries, and sink them to PostgreSQL as the other
            // collection results
            final var qRadarTag = TagRegistry.TAGS.get("qradar");
            qradarStream
                    .map(kafkaIPEntry -> new KafkaMergedResult(kafkaIPEntry.getDomainName(),
                            new KafkaDomainAggregate(kafkaIPEntry.getDomainName(), null, null, null, null),
                            // This must not be an immutable map to avoid issues with Kryo 2.24
                            new HashMap<>(Map.of(kafkaIPEntry.getIP(), new HashMap<>(Map.of(qRadarTag.byteValue(), kafkaIPEntry))))))
                    .name("Map QRadar results to merged results")
                    .uid("qradar-to-merged-results")
                    .sinkTo(postgresSink)
                    .name("Sink QRadar results to Postgres")
                    .uid("postgres-sink-qradar-collector-results");
        }

        // Sink the unified DN-IP data to Kafka
        var mergedData = mergedResults
                .map(new SerdeMappingFunction())
                .name("Map merged results to serialized tuples")
                .uid("serde-mapper-with-ips")
                .filter(tuple -> tuple.f0 != null)
                .name("Filter non-null results")
                .uid("result-with-ips-not-null-filter");
        mergedData
                .sinkTo(sink)
                .name("Sink merged results to Kafka")
                .uid("entries-with-ips-sink");

        // Handle the IP-less merged results from DN-based collectors
        var dnDataWithoutIps = dnData
                .filter(dnAggregate -> dnAggregate.getDNSIPs().isEmpty())
                .name("Filter DN data without IPs")
                .uid("dn-data-without-ips-filter")
                .keyBy(KafkaDomainAggregate::getDomainName);
        var resultsWithoutIpsKM = dnDataWithoutIps
                .map(dnAggregate -> new KafkaMergedResult(dnAggregate.getDomainName(), dnAggregate, null))
                .name("Map DN results to merged results")
                .uid("map-to-merged-results");

        if (postgresSink != null) {
            // Sink the unified DN-only metadata to PostgreSQL
            resultsWithoutIpsKM
                    .sinkTo(postgresSink)
                    .name("Sink collection results without IPs to Postgres")
                    .uid("postgres-sink-without-ips");
        }

        // Sink the unified DN-only data to Kafka
        var resultsWithoutIps = resultsWithoutIpsKM
                .map(new SerdeMappingFunction())
                .name("Map merged results without IPs to serialized tuples")
                .uid("serde-mapper-without-ips")
                .filter(tuple -> tuple.f0 != null)
                .name("Filter non-null results without IPs")
                .uid("result-without-ips-not-null-filter");
        resultsWithoutIps
                .sinkTo(sink)
                .name("Sink merged results to Kafka")
                .uid("entries-without-ips-sink");

        // ==== A side pipeline for QRadar data ====
        if (postgresQRadarSink != null) {
            qradarStream
                    .sinkTo(postgresQRadarSink)
                    .name("Sink QRadar results to Postgres")
                    .uid("postgres-sink-qradar-structured");
        }
    }

    private static KeyedStream<KafkaDomainEntry, String> makeKafkaDomainStream(final StreamExecutionEnvironment env,
                                                                               final String topic) {
        final var outOfOrdernessMs =
                Long.parseLong(appProperties.getProperty(MergerConfig.DN_MAX_OUT_OF_ORDERNESS_MS_CONFIG,
                        MergerConfig.DN_MAX_OUT_OF_ORDERNESS_MS_DEFAULT));
        final var idlenessSec =
                Long.parseLong(appProperties.getProperty(MergerConfig.DN_IDLENESS_SEC_CONFIG,
                        MergerConfig.DN_IDLENESS_SEC_DEFAULT));

        return env.fromSource(makeKafkaDomainSource(topic), makeWatermarkStrategy(outOfOrdernessMs, idlenessSec), "Kafka: " + topic)
                .uid("source-kafka-dn-" + topic)
                .keyBy(KafkaDomainEntry::getDomainName);
    }

    private static KeyedStream<KafkaIPEntry, String> makeKafkaIpStream(final StreamExecutionEnvironment env) {
        final var topic = Topics.OUT_IP;
        final var outOfOrdernessMs =
                Long.parseLong(appProperties.getProperty(MergerConfig.IP_MAX_OUT_OF_ORDERNESS_MS_CONFIG,
                        MergerConfig.IP_MAX_OUT_OF_ORDERNESS_MS_DEFAULT));
        final var idlenessSec =
                Long.parseLong(appProperties.getProperty(MergerConfig.IP_IDLENESS_SEC_CONFIG,
                        MergerConfig.IP_IDLENESS_SEC_DEFAULT));

        return env.fromSource(makeKafkaIpSource(topic), makeWatermarkStrategy(outOfOrdernessMs, idlenessSec), "Kafka: " + topic)
                .uid("source-kafka-ip-" + topic)
                .keyBy(KafkaIPEntry::getDomainName);
    }

    private static DataStream<KafkaIPEntry> makeKafkaQRadarStream(final StreamExecutionEnvironment env) {
        final var topic = Topics.OUT_QRADAR;
        final var outOfOrdernessMs =
                Long.parseLong(appProperties.getProperty(MergerConfig.IP_MAX_OUT_OF_ORDERNESS_MS_CONFIG,
                        MergerConfig.IP_MAX_OUT_OF_ORDERNESS_MS_DEFAULT));
        final var idlenessSec =
                Long.parseLong(appProperties.getProperty(MergerConfig.IP_IDLENESS_SEC_CONFIG,
                        MergerConfig.IP_IDLENESS_SEC_DEFAULT));

        return env.fromSource(makeKafkaQRadarSource(), makeWatermarkStrategy(outOfOrdernessMs, idlenessSec), "Kafka: " + topic)
                .uid("source-kafka-qradar-" + topic);
    }

    private static KafkaSource<KafkaDomainEntry> makeKafkaDomainSource(final String topic) {
        return KafkaSource.<KafkaDomainEntry>builder()
                .setProperties(kafkaProperties)
                .setTopics(topic)
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .setDeserializer(new KafkaDomainEntryDeserializer())
                .build();
    }

    private static KafkaSource<KafkaIPEntry> makeKafkaIpSource(final String topic) {
        return KafkaSource.<KafkaIPEntry>builder()
                .setProperties(kafkaProperties)
                .setTopics(topic)
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .setDeserializer(new KafkaIPEntryDeserializer())
                .build();
    }

    private static KafkaSource<KafkaIPEntry> makeKafkaQRadarSource() {
        return makeKafkaIpSource(Topics.OUT_QRADAR);
    }

    private static <T> WatermarkStrategy<T> makeWatermarkStrategy(long outOfOrdernessMs, long idlenessSec) {
        var strategy = WatermarkStrategy
                .<T>forBoundedOutOfOrderness(Duration.ofMillis(outOfOrdernessMs));

        if (idlenessSec > 0) {
            return strategy.withIdleness(Duration.ofSeconds(idlenessSec));
        } else {
            return strategy;
        }
    }
}
