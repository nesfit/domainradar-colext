package cz.vut.fit.domainradar.standalone.integration;

import com.fasterxml.jackson.core.type.TypeReference;
import cz.vut.fit.domainradar.CollectorConfig;
import cz.vut.fit.domainradar.Common;
import cz.vut.fit.domainradar.Topics;
import cz.vut.fit.domainradar.models.IPToProcess;
import cz.vut.fit.domainradar.models.ip.GeoIPData;
import cz.vut.fit.domainradar.models.requests.IPRequest;
import cz.vut.fit.domainradar.models.results.CommonIPResult;
import cz.vut.fit.domainradar.serialization.JsonSerde;
import cz.vut.fit.domainradar.standalone.collectors.GeoAsnCollector;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@Tag("integration")
class GeoAsnCollectorIntegrationTest {

    @Test
    void geoAsnCollectorEndToEnd() throws Exception {
        var brokers = IntegrationTestUtils.requireEnv("KAFKA_BROKERS");

        IntegrationTestUtils.assumeEnvMatchesOrSkip("KAFKA_INPUT_TOPIC", Topics.IN_IP,
                "KAFKA_INPUT_TOPIC must be " + Topics.IN_IP + " for GeoASN integration");
        IntegrationTestUtils.assumeEnvMatchesOrSkip("KAFKA_OUTPUT_TOPIC", Topics.OUT_IP,
                "KAFKA_OUTPUT_TOPIC must be " + Topics.OUT_IP + " for GeoASN integration");

        var geoDir = IntegrationTestUtils.requireEnv("GEOIP_DB_DIR");
        var geoPath = Path.of(geoDir);
        org.junit.jupiter.api.Assumptions.assumeTrue(Files.isDirectory(geoPath), "GEOIP_DB_DIR must be a directory");

        var inputTopic = IntegrationTestUtils.envOrDefault("KAFKA_INPUT_TOPIC", Topics.IN_IP);
        var outputTopic = IntegrationTestUtils.envOrDefault("KAFKA_OUTPUT_TOPIC", Topics.OUT_IP);

        var mapper = Common.makeMapper().build();
        var properties = IntegrationTestUtils.kafkaCollectorProperties(brokers);
        properties.put(CollectorConfig.GEOIP_DIRECTORY_CONFIG, geoPath.toString());
        var cityDb = System.getenv("GEOIP_CITY_DB");
        if (cityDb != null && !cityDb.isBlank()) {
            properties.put(CollectorConfig.GEOIP_CITY_DB_NAME_CONFIG, cityDb);
        }
        var asnDb = System.getenv("GEOIP_ASN_DB");
        if (asnDb != null && !asnDb.isBlank()) {
            properties.put(CollectorConfig.GEOIP_ASN_DB_NAME_CONFIG, asnDb);
        }

        var collector = new GeoAsnCollector(mapper, IntegrationTestUtils.randomAppId("geo-it"), properties);
        var collectorThread = IntegrationTestUtils.startCollector(collector);

        try (var producer = new KafkaProducer<>(
                properties,
                JsonSerde.of(mapper, IPToProcess.class).serializer(),
                JsonSerde.of(mapper, IPRequest.class).serializer());
             var consumer = new KafkaConsumer<>(
                     IntegrationTestUtils.kafkaConsumerProperties(brokers,
                             IntegrationTestUtils.randomAppId("geo-it-consumer")),
                     JsonSerde.of(mapper, IPToProcess.class).deserializer(),
                     JsonSerde.of(mapper, new TypeReference<CommonIPResult<GeoIPData>>() {
                     }).deserializer())) {

            consumer.subscribe(List.of(outputTopic));
            Thread.sleep(500);

            var ipToProcess = new IPToProcess("example.com", "8.8.8.8");
            var request = new IPRequest(List.of(GeoAsnCollector.NAME));
            producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>(inputTopic, ipToProcess, request)).get();
            producer.flush();

            var record = IntegrationTestUtils.waitForRecord(consumer,
                    entry -> ipToProcess.equals(entry.key()), IntegrationTestUtils.DEFAULT_TIMEOUT);

            assertNotNull(record);
            assertNotNull(record.value());
            assertNotNull(record.value().lastAttempt());
        } finally {
            IntegrationTestUtils.stopCollector(collector, collectorThread);
        }
    }
}
