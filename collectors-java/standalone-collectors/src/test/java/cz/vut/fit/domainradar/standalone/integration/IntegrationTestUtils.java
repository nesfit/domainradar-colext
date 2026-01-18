package cz.vut.fit.domainradar.standalone.integration;

import cz.vut.fit.domainradar.CollectorConfig;
import cz.vut.fit.domainradar.standalone.BaseStandaloneCollector;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Assumptions;

import java.time.Duration;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Predicate;

final class IntegrationTestUtils {
    static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(60);

    private IntegrationTestUtils() {
    }

    static String requireEnv(String name) {
        var value = System.getenv(name);
        Assumptions.assumeTrue(value != null && !value.isBlank(), name + " not set");
        return value;
    }

    static String envOrDefault(String name, String defaultValue) {
        var value = System.getenv(name);
        return (value == null || value.isBlank()) ? defaultValue : value;
    }

    static void assumeEnvMatchesOrSkip(String name, String expected, String reason) {
        var value = System.getenv(name);
        if (value == null || value.isBlank()) {
            return;
        }
        Assumptions.assumeTrue(expected.equals(value), reason);
    }

    static void assumeNetworkAllowed() {
        var value = System.getenv("TESTS_ALLOW_NETWORK");
        Assumptions.assumeTrue(value != null && !value.isBlank(), "TESTS_ALLOW_NETWORK not set");
    }

    static Properties kafkaCollectorProperties(String brokers) {
        var props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(CollectorConfig.MAX_CONCURRENCY_CONFIG, "1");
        props.put(CollectorConfig.CLOSE_TIMEOUT_SEC_CONFIG, "5");
        props.put(CollectorConfig.COMMIT_INTERVAL_MS_CONFIG, "250");
        return props;
    }

    static Properties kafkaConsumerProperties(String brokers, String groupId) {
        var props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        return props;
    }

    static String randomAppId(String prefix) {
        return prefix + "-" + UUID.randomUUID();
    }

    static CommandLine emptyCommandLine() throws Exception {
        return new DefaultParser().parse(new Options(), new String[0]);
    }

    static Thread startCollector(BaseStandaloneCollector<?, ?, ?> collector) throws Exception {
        var cmd = emptyCommandLine();
        var thread = new Thread(() -> collector.run(cmd),
                "collector-test-" + collector.getName() + "-" + UUID.randomUUID());
        thread.setDaemon(true);
        thread.start();
        return thread;
    }

    static void stopCollector(BaseStandaloneCollector<?, ?, ?> collector, Thread thread) throws Exception {
        try {
            collector.close();
        } finally {
            thread.interrupt();
            thread.join(5000);
        }
    }

    static <K, V> ConsumerRecord<K, V> waitForRecord(KafkaConsumer<K, V> consumer,
                                                     Predicate<ConsumerRecord<K, V>> matcher,
                                                     Duration timeout) {
        var deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            ConsumerRecords<K, V> records = consumer.poll(Duration.ofSeconds(1));
            for (var record : records) {
                if (matcher.test(record)) {
                    return record;
                }
            }
        }
        return null;
    }
}
