package cz.vut.fit.domainradar.streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.vut.fit.domainradar.Common;
import cz.vut.fit.domainradar.streams.mergers.CollectedDataMergerComponent;
import org.apache.commons.cli.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.TopologyException;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class StreamsPipelineRunner {
    private static final Logger Logger = LoggerFactory.getLogger(StreamsPipelineRunner.class);
    private static Properties Properties;

    public static void main(String[] args) {
        final var options = makeOptions();
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

        final Properties props = new Properties();
        Properties = props;

        if (cmd.hasOption("properties")) {
            // Open the file and load the properties
            var path = cmd.getOptionValue("properties");
            Logger.info("Loading properties from {}", path);
            try (var inStream = new FileInputStream(path)) {
                props.load(inStream);
            } catch (IOException e) {
                System.err.println("Failed to load properties: " + e.getMessage());
                System.exit(2);
                return;
            }
        }

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, cmd.getOptionValue("id"));
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, cmd.getOptionValue("bootstrap-server"));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                "org.apache.kafka.streams.errors.LogAndContinueExceptionHandler");

        final StreamsBuilder builder = new StreamsBuilder();
        final ObjectMapper jsonMapper = Common.makeMapper().build();
        List<PipelineComponent> components;

        try {
            components = populateBuilder(cmd, builder, jsonMapper);
            logComponents(components);
        } catch (Exception e) {
            Logger.error("Failed to initialize some of the pipeline components", e);
            System.exit(1);
            return;
        }

        runStreams(builder, components);
    }

    private static void runStreams(StreamsBuilder builder, List<PipelineComponent> components) {
        Logger.info("Running in Kafka Streams mode");

        final CountDownLatch latch = new CountDownLatch(1);

        final Topology topology = builder.build(Properties);
        Logger.info("Topology: {}", topology.describe());

        try (KafkaStreams streams = new KafkaStreams(topology, Properties)) {
            // attach shutdown handler to catch control-c
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                streams.close();
                latch.countDown();
            }, "streams-shutdown-hook"));

            try {
                streams.start();
                latch.await();
                closeComponents(components);
                System.exit(0);
            } catch (Throwable e) {
                Logger.error("Unhandled exception", e);
                System.exit(1);
            }
        } catch (TopologyException topologyException) {
            Logger.error(topologyException.getMessage());
        }
    }

    private static List<PipelineComponent> populateBuilder(CommandLine cmd, StreamsBuilder builder,
                                                           ObjectMapper jsonMapper) {
        // var useAllCollectors = cmd.hasOption("ac") || cmd.hasOption("a");
        var useAll = cmd.hasOption("a");
        var components = new ArrayList<PipelineComponent>();

        if (cmd.hasOption("merger") || useAll) {
            var merger = new CollectedDataMergerComponent(jsonMapper);
            merger.use(builder);
            components.add(merger);
        }

        return components;
    }

    private static void printHelpAndExit(Options options, int exitCode) {
        final var formatter = new HelpFormatter();
        formatter.printHelp(119,
                "domainradar-pipeline -id <Streams app ID> -s <Kafka bootstrap ip:port> [options]",
                "", options, "");
        System.exit(exitCode);
    }

    private static void logComponents(List<PipelineComponent> components) {
        for (var component : components) {
            Logger.info("Using component: {} ({})", component.getName(), Integer.toHexString(component.hashCode()));
        }
    }

    private static void closeComponents(List<PipelineComponent> components) {
        for (var component : components) {
            try {
                component.close();
            } catch (IOException e) {
                Logger.error("Error closing pipeline component {}", component.getName(), e);
            }
        }
    }

    @NotNull
    private static Options makeOptions() {
        final var options = new Options();
        options.addOption("a", "all", false, "Use all pipeline components");
        // options.addOption("ac", "all-collectors", false, "Use all collectors");

        options.addOption(null, "merger", false, "Use the collected data merger");

        options.addOption(Option.builder("properties")
                .longOpt("properties")
                .option("p")
                .desc("Path to a file with additional properties")
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

        return options;
    }
}