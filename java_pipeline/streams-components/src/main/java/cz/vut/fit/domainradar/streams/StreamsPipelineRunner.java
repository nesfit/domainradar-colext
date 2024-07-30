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

/**
 * The main class for running the Streams Pipeline.
 * This class initializes the logger and other necessary properties; then, it starts the Kafka Streams
 * application.
 */
public class StreamsPipelineRunner {
    private static final Logger Logger = LoggerFactory.getLogger(StreamsPipelineRunner.class);
    private static Properties Properties;

    public static void main(String[] args) {
        // Initialize and read the command line options
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

        // Read the properties
        final Properties props = new Properties();
        Properties = props;

        if (cmd.hasOption("properties")) {
            // Open the file and load the properties
            var path = cmd.getOptionValue("properties");
            Logger.info("Loading properties from {}", path);
            try (var inStream = new FileInputStream(path)) {
                props.load(inStream);
            } catch (IOException e) {
                Logger.error("Failed to load properties: {}", e.getMessage());
                System.exit(2);
                return;
            }
        }

        // Add properties from the command line
        final var cmdLineBootstrapServers = cmd.getOptionValue("bootstrap-server");
        if (cmdLineBootstrapServers != null) {
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, cmdLineBootstrapServers);
        }

        if (!props.containsKey(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG)) {
            Logger.error("Bootstrap servers not set. Use the {} property key or the -s option.",
                    StreamsConfig.BOOTSTRAP_SERVERS_CONFIG);
            System.exit(3);
            return;
        }

        var cmdLineProperties = cmd.getOptionValues("option");
        if (cmdLineProperties != null) {
            for (var option : cmdLineProperties) {
                if (option.contains("=")) {
                    var parts = option.split("=", 2);
                    props.put(parts[0], parts[1]);
                } else {
                    Logger.warn("Ignoring invalid command-line option: {}", option);
                }
            }
        }

        // Set the Kafka Streams app ID based on the command-line argument
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, cmd.getOptionValue("id"));
        // Set the default Serde to string
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        // Set the default deserialization exception handler to "log and continue"
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                "org.apache.kafka.streams.errors.LogAndContinueExceptionHandler");

        // Initialize the StreamsBuilder and populate it with the requested components
        final ObjectMapper jsonMapper = Common.makeMapper().build();
        final StreamsBuilder builder = new StreamsBuilder();
        List<PipelineComponent> components;

        try {
            components = populateBuilder(cmd, builder, jsonMapper);
            for (var component : components) {
                Logger.info("Using component: {} ({})", component.getName(), Integer.toHexString(component.hashCode()));
            }
        } catch (Exception e) {
            Logger.error("Failed to initialize some of the pipeline components", e);
            System.exit(1);
            return;
        }

        // Run the Streams app
        runStreams(builder, components);
    }

    private static void runStreams(StreamsBuilder builder, List<PipelineComponent> components) {
        Logger.info("Running in Kafka Streams mode");

        // A latch used to wait for the shutdown signal
        final CountDownLatch latch = new CountDownLatch(1);

        // Build the topology
        final Topology topology = builder.build(Properties);
        Logger.info("Topology: {}", topology.describe());

        try (KafkaStreams streams = new KafkaStreams(topology, Properties)) {
            // Register a shutdown hook to release the latch
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                streams.close();
                latch.countDown();
            }, "streams-shutdown-hook"));

            try {
                // Start the app and wait for the shutdown signal
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

    /**
     * Initializes the components requested in the command line options. Adds the components'
     * topologies to a given {@link StreamsBuilder}.
     *
     * @param cmd        The parsed command line arguments.
     * @param builder    The StreamsBuilder instance to use for building the topology.
     * @param jsonMapper The ObjectMapper instance to use for serialization and deserialization.
     * @return A list of initialized PipelineComponent instances.
     */
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

    /**
     * Prints the help message and exits the application.
     *
     * @param options  The Options instance containing the command line options.
     * @param exitCode The exit code to use.
     */
    private static void printHelpAndExit(Options options, int exitCode) {
        final var formatter = new HelpFormatter();
        formatter.printHelp(119,
                "domainradar-pipeline -id <Streams app ID> [options]",
                "", options, "");
        System.exit(exitCode);
    }

    /**
     * Closes the components from the given list.
     *
     * @param components The components to close.
     */
    private static void closeComponents(List<PipelineComponent> components) {
        for (var component : components) {
            try {
                component.close();
            } catch (IOException e) {
                Logger.error("Error closing pipeline component {}", component.getName(), e);
            }
        }
    }

    /**
     * Creates the main command line options.
     */
    @NotNull
    private static Options makeOptions() {
        final var options = new Options();
        options.addOption("a", "all", false, "Use all pipeline components");
        // options.addOption("ac", "all-collectors", false, "Use all collectors");

        options.addOption(null, "merger", false, "Use the collected data merger");

        options.addOption(Option.builder("properties")
                .longOpt("properties")
                .option("p")
                .desc("Path to a configuration file")
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
                .desc("Kafka bootstrap server IP:port")
                .argName("ip:port")
                .hasArg()
                .build()
        );
        options.addOption(Option.builder("o")
                .longOpt("option")
                .desc("A properties key/value to add to the configuration")
                .argName("key=value")
                .hasArg()
                .build()
        );

        options.addOption("h", "help", false, "Print this help message");

        return options;
    }
}