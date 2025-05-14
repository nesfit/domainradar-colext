package cz.vut.fit.domainradar.standalone.collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.net.InetAddresses;
import cz.vut.fit.domainradar.CollectorConfig;
import cz.vut.fit.domainradar.Common;
import cz.vut.fit.domainradar.Topics;
import cz.vut.fit.domainradar.models.IPToProcess;
import cz.vut.fit.domainradar.models.ResultCodes;
import cz.vut.fit.domainradar.models.repsystems.ProjectHoneypotData;
import cz.vut.fit.domainradar.models.requests.IPRequest;
import cz.vut.fit.domainradar.standalone.IPStandaloneCollector;
import io.confluent.parallelconsumer.ParallelStreamProcessor;
import org.apache.commons.cli.CommandLine;
import org.jetbrains.annotations.NotNull;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import pl.tlinkowski.unij.api.UniLists;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.*;

/**
 * A collector that scrapes and processes IP data from the ProjectHoneypot website.
 *
 * @author Matěj Čech
 */
public class ProjectHoneypotCollector extends IPStandaloneCollector<ProjectHoneypotData, ParallelStreamProcessor<IPToProcess, IPRequest>> {
    public static final String NAME = "project-honeypot";
    public static final String COMPONENT_NAME = "collector-" + NAME;
    private static final org.slf4j.Logger Logger = Common.getComponentLogger(ProjectHoneypotCollector.class);

    private static final String PROJECTHONEYPOT_BASE = "https://www.projecthoneypot.org/ip_";

    private final ExecutorService _executor;
    private final Duration _httpTimeout;

    public ProjectHoneypotCollector(ObjectMapper jsonMapper, String appName, Properties properties) {
        super(jsonMapper, appName, properties);
        _httpTimeout = Duration.ofSeconds(Integer.parseInt(
                properties.getProperty(CollectorConfig.PROJECTHONEYPOT_HTTP_TIMEOUT_CONFIG,
                        CollectorConfig.PROJECTHONEYPOT_HTTP_TIMEOUT_DEFAULT)));
        _executor = Executors.newVirtualThreadPerTaskExecutor();
    }

    @Override
    public void run(CommandLine cmd) {
        // Add a bit of a buffer to make the absolute processing timeout
        final var processingTimeout = (long) (_httpTimeout.toMillis() * 1.2);

        buildProcessor(0, processingTimeout);

        _parallelProcessor.subscribe(UniLists.of(Topics.IN_IP));
        _parallelProcessor.poll(ctx -> {
            var entry = ctx.getSingleConsumerRecord();
            var ip = entry.key();

            // Bounded the processing with the absolute processing timeout
            var processFuture = this.evaluateIP(ip)
                    .orTimeout(processingTimeout, TimeUnit.MILLISECONDS);

            try {
                processFuture.join();
            }
            catch (CompletionException e) {
                if (e.getCause() instanceof TimeoutException) {
                    Logger.debug("Operation timed out");
                }
                else {
                    Logger.warn("Unexpected error");
                }
            }
        });
    }

    /**
     * Asynchronously evaluates a given IP address using the ProjectHoneypot website.
     *
     * @param ip The IP address to process.
     * @return A CompletableFuture that completes when the evaluation is done.
     */
    protected CompletableFuture<Void> evaluateIP(IPToProcess ip) {
        String requestUrl = PROJECTHONEYPOT_BASE + ip.ip();
        final var ipInet = InetAddresses.forString(ip.ip());

        return CompletableFuture.runAsync(() -> {
            // IPv6 is not supported by ProjectHoneypot
            if (ipInet instanceof Inet6Address) {
                Logger.trace("Discarding IPv6 address: {}", ip.ip());
                _producer.send(resultRecord(Topics.OUT_IP, ip,
                        errorResult(ResultCodes.UNSUPPORTED_ADDRESS, null)));
            }
            else if (ipInet instanceof Inet4Address) {
                try {
                    Document document = Jsoup.connect(requestUrl).get();
                    Boolean malicious = parseMaliciousInfo(document);

                    Logger.trace("Success: {}", ip);

                    _producer.send(resultRecord(Topics.OUT_IP, ip,
                            successResult(new ProjectHoneypotData(malicious))));
                }
                catch (IOException e) {
                    Logger.debug("Cannot fetch");

                    _producer.send(resultRecord(Topics.OUT_IP, ip,
                            errorResult(ResultCodes.CANNOT_FETCH, getName())));
                }
            }
        });
    }

    /**
     * Parses the detection count from the ProjectHoneypot HTML report.
     *
     * @param document The Jsoup Document object representing the HTML page.
     * @return True if the IP is malicious to ProjectHoneypot, False is it is not malicious to ProjectHoneypot,
     * or null if ProjectHoneypot has no data about this IP.
     */
    private Boolean parseMaliciousInfo(Document document) {
        Element ipInfo = document.select("h2").first();
        if (ipInfo != null) {
            return !ipInfo.children().isEmpty();
        }

        return null;
    }

    @Override
    public @NotNull String getName() {
        return NAME;
    }

    /**
     * Builds the parallel stream processor.
     *
     * @param batchSize The batch size for processing records.
     * @param timeoutMs The base timeout guaranteed by the collection process, in milliseconds.
     */
    protected void buildProcessor(int batchSize, long timeoutMs) {
        _parallelProcessor = ParallelStreamProcessor.createEosStreamProcessor(
                this.buildProcessorOptions(batchSize, timeoutMs)
        );
    }

    @Override
    public void close() throws IOException {
        super.close();
        _executor.close();
    }
}
