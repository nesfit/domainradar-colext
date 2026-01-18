package cz.vut.fit.domainradar.standalone.integration;

import com.fasterxml.jackson.core.type.TypeReference;
import cz.vut.fit.domainradar.CollectorConfig;
import cz.vut.fit.domainradar.Common;
import cz.vut.fit.domainradar.Topics;
import cz.vut.fit.domainradar.models.IPToProcess;
import cz.vut.fit.domainradar.models.ip.QRadarData;
import cz.vut.fit.domainradar.models.requests.IPRequest;
import cz.vut.fit.domainradar.models.results.CommonIPResult;
import cz.vut.fit.domainradar.serialization.JsonSerde;
import cz.vut.fit.domainradar.standalone.collectors.VertxQRadarCollector;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@Tag("integration")
class QRadarCollectorIntegrationTest {

    @Test
    void qradarCollectorEndToEnd() throws Exception {
        var brokers = IntegrationTestUtils.requireEnv("KAFKA_BROKERS");
        IntegrationTestUtils.assumeNetworkAllowed();

        IntegrationTestUtils.assumeEnvMatchesOrSkip("KAFKA_INPUT_TOPIC", Topics.IN_IP,
                "KAFKA_INPUT_TOPIC must be " + Topics.IN_IP + " for QRadar integration");
        IntegrationTestUtils.assumeEnvMatchesOrSkip("KAFKA_OUTPUT_TOPIC", Topics.OUT_QRADAR,
                "KAFKA_OUTPUT_TOPIC must be " + Topics.OUT_QRADAR + " for QRadar integration");

        var qradarUrl = IntegrationTestUtils.requireEnv("QRADAR_URL");
        var qradarToken = IntegrationTestUtils.requireEnv("QRADAR_TOKEN");

        var inputTopic = IntegrationTestUtils.envOrDefault("KAFKA_INPUT_TOPIC", Topics.IN_IP);
        var outputTopic = IntegrationTestUtils.envOrDefault("KAFKA_OUTPUT_TOPIC", Topics.OUT_QRADAR);

        var mapper = Common.makeMapper().build();
        var properties = IntegrationTestUtils.kafkaCollectorProperties(brokers);
        properties.put(CollectorConfig.QRADAR_URL_CONFIG, qradarUrl);
        properties.put(CollectorConfig.QRADAR_TOKEN_CONFIG, qradarToken);
        properties.put(CollectorConfig.QRADAR_BATCH_SIZE_CONFIG, "1");

        var trustAll = System.getenv("QRADAR_TRUST_ALL");
        if (trustAll != null && !trustAll.isBlank()) {
            properties.put(CollectorConfig.QRADAR_TRUST_ALL_CONFIG, trustAll);
        }

        var collector = new VertxQRadarCollector(mapper, IntegrationTestUtils.randomAppId("qradar-it"), properties);
        var collectorThread = IntegrationTestUtils.startCollector(collector);

        try (var producer = new KafkaProducer<>(
                properties,
                JsonSerde.of(mapper, IPToProcess.class).serializer(),
                JsonSerde.of(mapper, IPRequest.class).serializer());
             var consumer = new KafkaConsumer<>(
                     IntegrationTestUtils.kafkaConsumerProperties(brokers,
                             IntegrationTestUtils.randomAppId("qradar-it-consumer")),
                     JsonSerde.of(mapper, IPToProcess.class).deserializer(),
                     JsonSerde.of(mapper, new TypeReference<CommonIPResult<QRadarData>>() {
                     }).deserializer())) {

            consumer.subscribe(List.of(outputTopic));
            Thread.sleep(500);

            var ipToProcess = new IPToProcess("example.com", "8.8.8.8");
            var request = new IPRequest(List.of(VertxQRadarCollector.NAME));
            producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>(inputTopic, ipToProcess, request)).get();
            producer.flush();

            var record = IntegrationTestUtils.waitForRecord(consumer,
                    entry -> ipToProcess.equals(entry.key()), Duration.ofSeconds(90));

            assertNotNull(record);
            assertNotNull(record.value());
            assertNotNull(record.value().lastAttempt());
        } finally {
            IntegrationTestUtils.stopCollector(collector, collectorThread);
        }
    }
}
