package cz.vut.fit.domainradar.standalone;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import cz.vut.fit.domainradar.CollectorConfig;
import cz.vut.fit.domainradar.standalone.collectors.DNSCollector;
import cz.vut.fit.domainradar.standalone.collectors.NERDCollector;
import cz.vut.fit.domainradar.standalone.collectors.ZoneCollector;
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

public class StandaloneCollectorRunner {
    private static final org.slf4j.Logger Logger = LoggerFactory.getLogger(StandaloneCollectorRunner.class);

    public static void main(String[] args) {
        final var options = makeInitialOptions();

        CommandLine cmd;
        cmd = parseCommandLine(args, options, false);
        if (cmd == null) return;

        final ObjectMapper jsonMapper = JsonMapper.builder()
                .addModule(new JavaTimeModule())
                .configure(JsonParser.Feature.INCLUDE_SOURCE_IN_LOCATION, true)
                .build();
        final Properties properties = initProperties(cmd);

        if (cmd.hasOption("mc")) {
            properties.setProperty(CollectorConfig.MAX_CONCURRENCY_CONFIG, cmd.getOptionValue("mc"));
        }

        var toRun = initCollectors(cmd, jsonMapper, properties);

        if (toRun.isEmpty()) {
            System.err.println("No collectors selected");
            printHelpAndExit(options, 1);
            return;
        }

        // Add per-component command-line settings and run the command line parser again
        toRun.forEach(collector -> collector.addOptions(options));
        final var cmdExtended = parseCommandLine(args, options, true);
        if (cmdExtended == null) return;

        for (var component : toRun) {
            Logger.info("Using component: {} ({})", component.getName(), Integer.toHexString(component.hashCode()));
        }

        final CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(latch::countDown, "system-shutdown-hook"));
        toRun.forEach(collector -> collector.run(cmdExtended));
        try {
            latch.await();
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

    private static List<BaseStandaloneCollector<?, ?, ?, ?>> initCollectors(CommandLine cmd, ObjectMapper mapper,
                                                                            Properties properties) {
        var appId = cmd.getOptionValue("id");
        var useAll = cmd.hasOption("a");
        var components = new ArrayList<BaseStandaloneCollector<?, ?, ?, ?>>();

        try {
            if (useAll || cmd.hasOption("col-dns")) {
                components.add(new DNSCollector(mapper, appId, properties));
            }

            if (useAll || cmd.hasOption("col-zone")) {
                components.add(new ZoneCollector(mapper, appId, properties));
            }

            if (useAll || cmd.hasOption("col-nerd")) {
                components.add(new NERDCollector(mapper, appId, properties));
            }
        } catch (Exception e) {
            Logger.error("Failed to initialize a collector", e);
            System.exit(4);
        }

        return components;
    }

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

    @NotNull
    private static Options makeInitialOptions() {
        final var options = new Options();
        options.addOption("h", "help", false, "Print this help message");
        options.addOption("a", "all", false, "Use all collectors");

        options.addOption(null, "col-dns", false, "Use the DNS+TLS collector");
        options.addOption(null, "col-zone", false, "Use the zone collector");
        options.addOption(null, "col-nerd", false, "Use the NERD collector");

        options.addOption(Option.builder("id")
                .longOpt("app-id")
                .desc("Application ID (required)")
                .argName("id")
                .hasArg()
                .required()
                .build());
        options.addOption(Option.builder("s")
                .longOpt("bootstrap-servers")
                .desc("Kafka bootstrap server IP:port, separated by commas (required)")
                .argName("ip:port")
                .hasArg()
                .required()
                .build());
        options.addOption(Option.builder("mc")
                .longOpt("max-concurrency")
                .desc("Maximum concurrency")
                .argName("num of concurrent requests")
                .hasArg()
                .type(Integer.class)
                .build());
        options.addOption(Option.builder("p")
                .longOpt("properties")
                .desc("Path to a file with additional properties")
                .argName("path")
                .hasArg()
                .build());

        return options;
    }

    private static Properties initProperties(CommandLine cmd) {
        final Properties props = new Properties();

        if (cmd.hasOption("properties")) {
            // Open the file and load the properties
            var path = cmd.getOptionValue("properties");
            try (var inStream = new FileInputStream(path)) {
                props.load(inStream);
            } catch (IOException e) {
                System.err.println("Failed to load properties: " + e.getMessage());
                System.exit(2);
                return null;
            }
        }

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cmd.getOptionValue("bootstrap-servers"));
        return props;
    }

    private static void printHelpAndExit(Options options, int exitCode) {
        final var formatter = new HelpFormatter();
        formatter.printHelp(119,
                "domrad-collector -id <App ID> -s <Kafka bootstrap ip:port> [options]",
                "", options, "");
        System.exit(exitCode);
    }
}
