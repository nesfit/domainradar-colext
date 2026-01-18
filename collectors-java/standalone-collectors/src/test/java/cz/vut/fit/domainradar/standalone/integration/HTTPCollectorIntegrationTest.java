package cz.vut.fit.domainradar.standalone.integration;

import cz.vut.fit.domainradar.Common;
import cz.vut.fit.domainradar.CollectorConfig;
import cz.vut.fit.domainradar.Topics;
import cz.vut.fit.domainradar.models.results.HTTPResult;
import cz.vut.fit.domainradar.serialization.JsonSerde;
import cz.vut.fit.domainradar.standalone.collectors.VertxHTTPCollector;
import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.FixMethodOrder;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.*;

@Tag("integration")
@FixMethodOrder
class HTTPCollectorIntegrationTest {

    @Test
    void httpCollectorEndToEnd() throws Exception {
        var brokers = IntegrationTestUtils.requireEnv("KAFKA_BROKERS");
        IntegrationTestUtils.assumeNetworkAllowed();

        IntegrationTestUtils.assumeEnvMatchesOrSkip("KAFKA_INPUT_TOPIC", Topics.IN_HTTP,
                "KAFKA_INPUT_TOPIC must be " + Topics.IN_HTTP + " for HTTP integration");
        IntegrationTestUtils.assumeEnvMatchesOrSkip("KAFKA_OUTPUT_TOPIC", Topics.OUT_HTTP,
                "KAFKA_OUTPUT_TOPIC must be " + Topics.OUT_HTTP + " for HTTP integration");

        var inputTopic = IntegrationTestUtils.envOrDefault("KAFKA_INPUT_TOPIC", Topics.IN_HTTP);
        var outputTopic = IntegrationTestUtils.envOrDefault("KAFKA_OUTPUT_TOPIC", Topics.OUT_HTTP);

        var mapper = Common.makeMapper().build();
        var properties = IntegrationTestUtils.kafkaCollectorProperties(brokers);
        var collector = new VertxHTTPCollector(mapper, IntegrationTestUtils.randomAppId("http-it"), properties);
        var collectorThread = IntegrationTestUtils.startCollector(collector);

        try (var producer = new KafkaProducer<>(properties, new StringSerializer(), new StringSerializer());
             var consumer = new KafkaConsumer<>(
                     IntegrationTestUtils.kafkaConsumerProperties(brokers,
                             IntegrationTestUtils.randomAppId("http-it-consumer")),
                     new StringDeserializer(),
                     JsonSerde.of(mapper, HTTPResult.class).deserializer())) {

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

    @Test
    void httpCollectorUsesLocalServer() throws Exception {
        final var body = "ok";
        final var bodyBytes = body.getBytes(StandardCharsets.UTF_8);

        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            exchange.sendResponseHeaders(200, bodyBytes.length);
            try (var out = exchange.getResponseBody()) {
                out.write(bodyBytes);
            }
        });
        server.setExecutor(Executors.newSingleThreadExecutor());
        server.start();

        var brokers = IntegrationTestUtils.requireEnv("KAFKA_BROKERS");
        var port = server.getAddress().getPort();
        var mapper = Common.makeMapper().build();
        var properties = IntegrationTestUtils.kafkaCollectorProperties(brokers);
        properties.put(CollectorConfig.HTTP_PORT_CONFIG, Integer.toString(port));
        properties.put(CollectorConfig.HTTP_TIMEOUT_MS_CONFIG, "500");

        var collector = new VertxHTTPCollector(mapper, IntegrationTestUtils.randomAppId("http-local-it"), properties);
        var collectorThread = IntegrationTestUtils.startCollector(collector);

        try (var producer = new KafkaProducer<>(properties, new StringSerializer(), new StringSerializer());
             var consumer = new KafkaConsumer<>(
                     IntegrationTestUtils.kafkaConsumerProperties(brokers,
                             IntegrationTestUtils.randomAppId("http-local-it-consumer")),
                     new StringDeserializer(),
                     JsonSerde.of(mapper, HTTPResult.class).deserializer())) {

            consumer.subscribe(java.util.List.of(Topics.OUT_HTTP));
            Thread.sleep(500);

            var domain = "local.test";
            var ip = "127.0.0.1";
            producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>(Topics.IN_HTTP, domain, ip)).get();
            producer.flush();

            var record = IntegrationTestUtils.waitForRecord(consumer,
                    entry -> domain.equals(entry.key()), IntegrationTestUtils.DEFAULT_TIMEOUT);

            assertNotNull(record);
            assertNotNull(record.value());
            assertEquals(200, record.value().finalHttpCode());
            assertEquals("http://" + ip + ":" + port + "/", record.value().targetUrl());
            assertArrayEquals(record.value().html(), bodyBytes);
        } finally {
            IntegrationTestUtils.stopCollector(collector, collectorThread);
            server.stop(0);
        }
    }
}
