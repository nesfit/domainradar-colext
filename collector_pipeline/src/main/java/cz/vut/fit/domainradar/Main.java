package cz.vut.fit.domainradar;

import cz.vut.fit.domainradar.pipeline.collectors.*;
import cz.vut.fit.domainradar.pipeline.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.commons.cli.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py4j.GatewayServer;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;

public class Main {
    private static final Logger Logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        final var pythonGateway = new GatewayServer(null);
        pythonGateway.start();
        final var pythonEntryPoint = (PythonEntryPoint) pythonGateway.getPythonServerEntryPoint(
                new Class[]{PythonEntryPoint.class});

        final var options = new Options();
        options.addOption("a", "all", false, "Use all pipeline components");
        options.addOption("ac", "all-collectors", false, "Use all collectors");

        options.addOption(null, "col-dns", false, "Use the DNS+TLS collector");
        options.addOption(null, "col-geoip", false, "Use the GeoIP collector");
        options.addOption(null, "col-nerd", false, "Use the NERD collector");
        options.addOption(Option.builder()
                .longOpt("col-ping")
                .desc("Use the Ping/RTT collector")
                .argName("collector ID")
                .hasArg()
                .build());
        options.addOption(null, "col-rdap-dn", false, "Use the RDAP-DN collector");
        options.addOption(null, "col-rdap-ip", false, "Use the RDAP-IP collector");
        options.addOption(null, "col-zone", false, "Use the zone collector");
        options.addOption(null, "merger", false, "Use the DNS/IP merger");
        options.addOption(Option.builder("threads")
                .option("t")
                .longOpt("threads")
                .desc("Number of threads to use")
                .argName("num of threads")
                .hasArg()
                .type(Integer.class)
                .build());
        options.addOption(Option.builder("properties")
                .longOpt("properties")
                .option("p")
                .desc("Path to a file with additional Kafka Streams properties")
                .argName("path")
                .hasArg()
                .build());
        options.addOption(Option.builder("id")
                .longOpt("app-id")
                .desc("Kafka Streams application ID (required)")
                .argName("id")
                .hasArg()
                .required()
                .build()
        );
        options.addOption(Option.builder("s")
                .longOpt("bootstrap-server")
                .desc("Kafka bootstrap server IP:port (required)")
                .argName("ip:port")
                .hasArg()
                .required()
                .build()
        );
        options.addOption("h", "help", false, "Print this help message");

        final var parser = new DefaultParser();
        CommandLine cmd;
        try {
            cmd = parser.parse(options, args, true);
        } catch (ParseException e) {
            if (Arrays.stream(args).anyMatch(arg -> arg.equals("-h") || arg.equals("--help"))) {
                printHelpAndExit(options, 0);
                return;
            }

            System.err.println(e.getMessage());
            printHelpAndExit(options, 1);
            return;
        }

        if (cmd.hasOption("h")) {
            printHelpAndExit(options, 0);
            return;
        }

        final Properties ksProperties = new Properties();
        if (cmd.hasOption("properties")) {
            // Open the file and load the properties
            var path = cmd.getOptionValue("properties");
            Logger.info("Loading properties from {}", path);
            try (var inStream = new FileInputStream(path)) {
                ksProperties.load(inStream);
            } catch (IOException e) {
                System.err.println("Failed to load properties: " + e.getMessage());
                System.exit(1);
                return;
            }
        }

        int threads = 4;
        if (cmd.hasOption("threads")) {
            threads = Integer.parseInt(cmd.getOptionValue("threads"));
        }

        ksProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, cmd.getOptionValue("id"));
        ksProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, cmd.getOptionValue("bootstrap-server"));
        ksProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        ksProperties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, threads);
        ksProperties.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                "org.apache.kafka.streams.errors.LogAndContinueExceptionHandler");

        final StreamsBuilder builder = new StreamsBuilder();
        final ObjectMapper jsonMapper = JsonMapper.builder()
                .addModule(new JavaTimeModule())
                .build();

        populateBuilder(cmd, builder, jsonMapper, pythonEntryPoint);

        final Topology topology = builder.build(ksProperties);
        Logger.info("Topology: {}", topology.describe());

        final CountDownLatch latch = new CountDownLatch(1);
        try (KafkaStreams streams = new KafkaStreams(topology, ksProperties)) {
            // attach shutdown handler to catch control-c
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                streams.close();
                latch.countDown();
            }, "streams-shutdown-hook"));

            try {
                streams.start();
                latch.await();
                pythonGateway.shutdown();
                System.exit(0);
            } catch (Throwable e) {
                Logger.error("Unhandled exception", e);
                System.exit(1);
            }
        }
    }

    private static void printHelpAndExit(Options options, int exitCode) {
        final var formatter = new HelpFormatter();
        formatter.printHelp(119,
                "domainradar-pipeline -id <Streams app ID> -s <Kafka bootstrap ip:port> [options]",
                "", options, "");
        System.exit(exitCode);
    }

    private static void populateBuilder(CommandLine cmd, StreamsBuilder builder,
                                        ObjectMapper jsonMapper, PythonEntryPoint pythonEntryPoint) {
        var useAllCollectors = cmd.hasOption("ac") || cmd.hasOption("a");
        var useAll = cmd.hasOption("a");

        if (cmd.hasOption("col-dns") || useAllCollectors) {
            var dnsCollector = new DNSCollector(jsonMapper);
            dnsCollector.addTo(builder);
        }

        if (cmd.hasOption("col-geoip") || useAllCollectors) {
            var geoIpCollector = new GeoIPCollector(jsonMapper);
            geoIpCollector.addTo(builder);
        }

        if (cmd.hasOption("col-nerd") || useAllCollectors) {
            var nerdCollector = new NERDCollector(jsonMapper);
            nerdCollector.addTo(builder);
        }

        if (cmd.hasOption("col-ping") || useAllCollectors) {
            var pingIds = cmd.getOptionValues("col-ping");
            if (pingIds == null || pingIds.length == 0) {
                var pingCollector = new PingCollector(jsonMapper, "default");
                pingCollector.addTo(builder);
            } else {
                for (var id : pingIds) {
                    var pingCollector = new PingCollector(jsonMapper, id);
                    pingCollector.addTo(builder);
                }
            }
        }

        if (cmd.hasOption("col-rdap-dn") || useAllCollectors) {
            var rdapDnCollector = new RDAPDomainCollector(jsonMapper);
            rdapDnCollector.addTo(builder);
        }

        if (cmd.hasOption("col-rdap-ip") || useAllCollectors) {
            var rdapIpCollector = new RDAPInetAddressCollector(jsonMapper);
            rdapIpCollector.addTo(builder);
        }

        if (cmd.hasOption("col-zone") || useAllCollectors) {
            var zoneCollector = new ZoneCollector(jsonMapper, pythonEntryPoint);
            zoneCollector.addTo(builder);
        }

        if (cmd.hasOption("merger") || useAll) {
            var merger = new IPDataMergerComponent(jsonMapper);
            merger.addTo(builder);
        }
    }
}