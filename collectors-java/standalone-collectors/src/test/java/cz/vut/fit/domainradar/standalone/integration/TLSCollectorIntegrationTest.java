package cz.vut.fit.domainradar.standalone.integration;

import cz.vut.fit.domainradar.Common;
import cz.vut.fit.domainradar.Topics;
import cz.vut.fit.domainradar.models.results.TLSResult;
import cz.vut.fit.domainradar.serialization.JsonSerde;
import cz.vut.fit.domainradar.standalone.collectors.TLSCollector;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@Tag("integration")
class TLSCollectorIntegrationTest {

    @Test
    void tlsCollectorEndToEnd() throws Exception {
        var brokers = IntegrationTestUtils.requireEnv("KAFKA_BROKERS");
        IntegrationTestUtils.assumeNetworkAllowed();

        IntegrationTestUtils.assumeEnvMatchesOrSkip("KAFKA_INPUT_TOPIC", Topics.IN_TLS,
                "KAFKA_INPUT_TOPIC must be " + Topics.IN_TLS + " for TLS integration");
        IntegrationTestUtils.assumeEnvMatchesOrSkip("KAFKA_OUTPUT_TOPIC", Topics.OUT_TLS,
                "KAFKA_OUTPUT_TOPIC must be " + Topics.OUT_TLS + " for TLS integration");

        var inputTopic = IntegrationTestUtils.envOrDefault("KAFKA_INPUT_TOPIC", Topics.IN_TLS);
        var outputTopic = IntegrationTestUtils.envOrDefault("KAFKA_OUTPUT_TOPIC", Topics.OUT_TLS);

        var mapper = Common.makeMapper().build();
        var properties = IntegrationTestUtils.kafkaCollectorProperties(brokers);
        var collector = new TLSCollector(mapper, IntegrationTestUtils.randomAppId("tls-it"), properties);
        var collectorThread = IntegrationTestUtils.startCollector(collector);

        try (var producer = new KafkaProducer<>(properties, new StringSerializer(), new StringSerializer());
             var consumer = new KafkaConsumer<>(
                     IntegrationTestUtils.kafkaConsumerProperties(brokers,
                             IntegrationTestUtils.randomAppId("tls-it-consumer")),
                     new StringDeserializer(),
                     JsonSerde.of(mapper, TLSResult.class).deserializer())) {

            consumer.subscribe(java.util.List.of(outputTopic));
            Thread.sleep(500);

            var domain = "example.com";
            var ip = "93.184.216.34";
            producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>(inputTopic, domain, ip)).get();
            producer.flush();

            var record = IntegrationTestUtils.waitForRecord(consumer,
                    entry -> domain.equals(entry.key()), IntegrationTestUtils.DEFAULT_TIMEOUT);

            assertNotNull(record);
            assertNotNull(record.value());
            assertNotNull(record.value().lastAttempt());
        } finally {
            IntegrationTestUtils.stopCollector(collector, collectorThread);
        }
    }
}
