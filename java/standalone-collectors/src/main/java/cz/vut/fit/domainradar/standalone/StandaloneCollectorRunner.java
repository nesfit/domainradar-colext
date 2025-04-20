package cz.vut.fit.domainradar.standalone;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.vut.fit.domainradar.Common;
import cz.vut.fit.domainradar.standalone.collectors.AbuseIpDbCollector;
import cz.vut.fit.domainradar.standalone.collectors.GeoAsnCollector;
import cz.vut.fit.domainradar.standalone.collectors.NERDCollector;
import cz.vut.fit.domainradar.standalone.collectors.TLSCollector;
import cz.vut.fit.domainradar.standalone.collectors.VertxQRadarCollector;
import org.apache.commons.cli.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * The main class for running standalone collectors.
 * <p>
 * This class initializes the command line options, parses the arguments,
 * and runs the selected collectors.
 */
public class StandaloneCollectorRunner {
    private static final org.slf4j.Logger Logger = LoggerFactory.getLogger(StandaloneCollectorRunner.class);

    public static void main(String[] args) {
        // Initialize the command line options
        final var options = makeInitialOptions();

        CommandLine cmd;
        cmd = parseCommandLine(args, options, false); // ignore unknown options for the first run
        if (cmd == null) return;

        final ObjectMapper jsonMapper = Common.makeMapper().build();
        final Properties properties = initProperties(cmd);

        // Initialize the collectors based on the command line options
        var toRun = initCollectors(cmd, jsonMapper, properties);

        if (toRun.isEmpty()) {
            Logger.error("No collectors selected");
            printHelpAndExit(options, 1);
            return;
        }

        // Add per-component command-line settings and run the command line parser again
        toRun.forEach(collector -> collector.addOptions(options));
        final var cmdExtended = parseCommandLine(args, options, true);
        if (cmdExtended == null) return;

        for (var component : toRun) {
            Logger.info("Using collector: {} ({})", component.getName(), Integer.toHexString(component.hashCode()));
        }

        // A latch used to wait for the shutdown signal
        final CountDownLatch latch = new CountDownLatch(1);
        // Register a shutdown hook to release the latch
        Runtime.getRuntime().addShutdownHook(new Thread(latch::countDown, "system-shutdown-hook"));

        // Run the collectors (this does not block)
        toRun.forEach(collector -> collector.run(cmdExtended));

        try {
            // Wait for the shutdown signal
            latch.await();
            Logger.info("Exiting");
            // Close the collectors
            for (var component : toRun) {
                try {
                    component.close();
                } catch (Exception e) {
                    Logger.warn("Failed to close component {}", component.getName(), e);
                }
            }
            System.exit(0);
        } catch (InterruptedException e) {
            Logger.error("Unhandled exception", e);
            System.exit(1);
        }
    }

    /**
     * Initializes the list of collectors based on the command line options.
     *
     * @param cmd        The parsed command line arguments.
     * @param mapper     The ObjectMapper instance to use for serialization and deserialization.
     * @param properties The Properties instance containing configuration settings.
     * @return A list of initialized collector instances that implement the CollectorInterface.
     */
    @NotNull
    private static List<CollectorInterface> initCollectors(CommandLine cmd, ObjectMapper mapper, Properties properties) {
        var appId = cmd.getOptionValue("id");
        var useAll = cmd.hasOption("a");
        var components = new ArrayList<CollectorInterface>();

        try {
            if (useAll || cmd.hasOption("col-tls")) {
                components.add(new TLSCollector(mapper, appId, properties));
            }

            if (useAll || cmd.hasOption("col-nerd")) {
                components.add(new NERDCollector(mapper, appId, properties));
            }

            if (useAll || cmd.hasOption("col-geo")) {
                components.add(new GeoAsnCollector(mapper, appId, properties));
            }

            if (useAll || cmd.hasOption("col-qradar")) {
                components.add(new VertxQRadarCollector(mapper, appId, properties));
            }

            if (useAll || cmd.hasOption("col-abuseipdb")) {
                components.add(new AbuseIpDbCollector(mapper, appId, properties));
            }
        } catch (Exception e) {
            Logger.error("Failed to initialize a collector", e);
            System.exit(4);
        }

        return components;
    }

    /**
     * Parses the command line arguments using the specified options.
     *
     * @param args            The command line arguments to parse.
     * @param options         The Options instance containing the command line options.
     * @param stopAtNonOption If true, parsing will stop at the first non-option argument.
     * @return The parsed CommandLine instance, or null if parsing fails or help is requested.
     */
    @Nullable
    private static CommandLine parseCommandLine(String[] args, Options options, boolean stopAtNonOption) {
        final var parser = new DefaultParser();

        CommandLine cmd;
        try {
            cmd = parser.parse(options, args, stopAtNonOption);
        } catch (ParseException e) {
            if (Arrays.stream(args).anyMatch(arg -> arg.equals("-h") || arg.equals("--help"))) {
                printHelpAndExit(options, 0);
                return null;
            }

            System.err.println(e.getMessage());
            printHelpAndExit(options, 1);
            return null;
        }

        if (cmd.hasOption("h")) {
            printHelpAndExit(options, 0);
            return null;
        }
        return cmd;
    }

    /**
     * Creates the main command line options.
     */
    @NotNull
    private static Options makeInitialOptions() {
        final var options = new Options();
        options.addOption("h", "help", false, "Print this help message");
        options.addOption("a", "all", false, "Use all collectors");

        options.addOption(null, "col-tls", false, "Use the TLS collector");
        options.addOption(null, "col-nerd", false, "Use the NERD collector");
        options.addOption(null, "col-geo", false, "Use the GEO-ASN collector");
        options.addOption(null, "col-qradar", false, "Use the QRadar Offense collector");
        options.addOption(null, "col-abuseipdb", false, "Use the AbuseIPDB collector");

        options.addOption(Option.builder("id")
                .longOpt("app-id")
                .desc("Consumer group ID prefix (required)")
                .argName("id")
                .hasArg()
                .required()
                .build());
        options.addOption(Option.builder("s")
                .longOpt("bootstrap-server")
                .desc("Kafka bootstrap server(s) IP:port, separated by commas")
                .argName("ip:port")
                .hasArg()
                .build());
        options.addOption(Option.builder("p")
                .longOpt("properties")
                .desc("Path to a configuration file")
                .argName("path")
                .hasArg()
                .build());
        options.addOption(Option.builder("o")
                .longOpt("option")
                .desc("A properties key/value to add to the configuration")
                .argName("key=value")
                .hasArg()
                .build()
        );

        return options;
    }


    /**
     * Initializes the properties from the file and the --option passed in the command line.
     *
     * @param cmd The parsed command line arguments.
     * @return The initialized Properties instance.
     */
    private static Properties initProperties(CommandLine cmd) {
        final Properties props = new Properties();

        if (cmd.hasOption("properties")) {
            // Open the file and load the properties
            var path = cmd.getOptionValue("properties");
            try (var inStream = new FileInputStream(path)) {
                props.load(inStream);
            } catch (IOException e) {
                Logger.error("Failed to load properties: {}", e.getMessage());
                System.exit(2);
                return null;
            }
        }

        final var cmdLineBootstrapServers = cmd.getOptionValue("bootstrap-server");
        if (cmdLineBootstrapServers != null) {
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cmdLineBootstrapServers);
        }

        // Add the --option properties
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

        if (!props.containsKey(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)) {
            Logger.error("Bootstrap servers not set. Use the {} property key or the -s option.",
                    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG);
            System.exit(3);
            return null;
        }
        return props;
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
                "domrad-collector -id <consumer group prefix> [options]",
                "", options, "");
        System.exit(exitCode);
    }
}
