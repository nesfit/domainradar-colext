package cz.vut.fit.domainradar.standalone.integration;

import com.fasterxml.jackson.core.type.TypeReference;
import cz.vut.fit.domainradar.CollectorConfig;
import cz.vut.fit.domainradar.Common;
import cz.vut.fit.domainradar.Topics;
import cz.vut.fit.domainradar.models.IPToProcess;
import cz.vut.fit.domainradar.models.ip.NERDData;
import cz.vut.fit.domainradar.models.requests.IPRequest;
import cz.vut.fit.domainradar.models.results.CommonIPResult;
import cz.vut.fit.domainradar.serialization.JsonSerde;
import cz.vut.fit.domainradar.standalone.collectors.NERDCollector;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@Tag("integration")
class NERDCollectorIntegrationTest {

    @Test
    void nerdCollectorEndToEnd() throws Exception {
        var brokers = IntegrationTestUtils.requireEnv("KAFKA_BROKERS");
        IntegrationTestUtils.assumeNetworkAllowed();

        IntegrationTestUtils.assumeEnvMatchesOrSkip("KAFKA_INPUT_TOPIC", Topics.IN_IP,
                "KAFKA_INPUT_TOPIC must be " + Topics.IN_IP + " for NERD integration");
        IntegrationTestUtils.assumeEnvMatchesOrSkip("KAFKA_OUTPUT_TOPIC", Topics.OUT_IP,
                "KAFKA_OUTPUT_TOPIC must be " + Topics.OUT_IP + " for NERD integration");

        var nerdToken = IntegrationTestUtils.requireEnv("NERD_TOKEN");

        var inputTopic = IntegrationTestUtils.envOrDefault("KAFKA_INPUT_TOPIC", Topics.IN_IP);
        var outputTopic = IntegrationTestUtils.envOrDefault("KAFKA_OUTPUT_TOPIC", Topics.OUT_IP);

        var mapper = Common.makeMapper().build();
        var properties = IntegrationTestUtils.kafkaCollectorProperties(brokers);
        properties.put(CollectorConfig.NERD_TOKEN_CONFIG, nerdToken);
        properties.put(CollectorConfig.NERD_BATCH_SIZE_CONFIG, "1");
        properties.put(CollectorConfig.NERD_HTTP_TIMEOUT_CONFIG, "5");

        var collector = new NERDCollector(mapper, IntegrationTestUtils.randomAppId("nerd-it"), properties);
        var collectorThread = IntegrationTestUtils.startCollector(collector);

        try (var producer = new KafkaProducer<>(
                properties,
                JsonSerde.of(mapper, IPToProcess.class).serializer(),
                JsonSerde.of(mapper, IPRequest.class).serializer());
             var consumer = new KafkaConsumer<>(
                     IntegrationTestUtils.kafkaConsumerProperties(brokers,
                             IntegrationTestUtils.randomAppId("nerd-it-consumer")),
                     JsonSerde.of(mapper, IPToProcess.class).deserializer(),
                     JsonSerde.of(mapper, new TypeReference<CommonIPResult<NERDData>>() {
                     }).deserializer())) {

            consumer.subscribe(List.of(outputTopic));
            Thread.sleep(500);

            var ipToProcess = new IPToProcess("example.com", "8.8.8.8");
            var request = new IPRequest(List.of(NERDCollector.NAME));
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
