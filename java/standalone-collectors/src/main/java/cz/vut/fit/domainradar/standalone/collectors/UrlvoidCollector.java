package cz.vut.fit.domainradar.standalone.collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.vut.fit.domainradar.CollectorConfig;
import cz.vut.fit.domainradar.Common;
import cz.vut.fit.domainradar.Topics;
import cz.vut.fit.domainradar.models.DNToProcess;
import cz.vut.fit.domainradar.models.ResultCodes;
import cz.vut.fit.domainradar.models.repsystems.UrlvoidData;
import cz.vut.fit.domainradar.models.requests.DNRequest;
import cz.vut.fit.domainradar.standalone.DNStandaloneCollector;
import io.confluent.parallelconsumer.ParallelStreamProcessor;
import org.apache.commons.cli.CommandLine;
import org.jetbrains.annotations.NotNull;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import pl.tlinkowski.unij.api.UniLists;

import java.io.IOException;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.*;

/**
 * A collector that scrapes and processes DN data from the URLVoid website.
 *
 * @author Matěj Čech
 */
public class UrlvoidCollector extends DNStandaloneCollector<UrlvoidData, ParallelStreamProcessor<DNToProcess, DNRequest>> {
    public static final String NAME = "urlvoid";
    public static final String COMPONENT_NAME = "collector-" + NAME;
    private static final org.slf4j.Logger Logger = Common.getComponentLogger(UrlvoidCollector.class);

    private static final String URLVOID_BASE = "https://www.urlvoid.com/scan/";

    private final ExecutorService _executor;
    private final Duration _httpTimeout;

    public UrlvoidCollector(ObjectMapper jsonMapper, String appName, Properties properties) {
        super(jsonMapper, appName, properties);
        _httpTimeout = Duration.ofSeconds(Integer.parseInt(
                properties.getProperty(CollectorConfig.URLVOID_HTTP_TIMEOUT_CONFIG,
                        CollectorConfig.URLVOID_HTTP_TIMEOUT_DEFAULT)));
        _executor = Executors.newVirtualThreadPerTaskExecutor();
    }

    @Override
    public void run(CommandLine cmd) {
        // Add a bit of a buffer to make the absolute processing timeout
        final var processingTimeout = (long) (_httpTimeout.toMillis() * 1.2);

        buildProcessor(0, processingTimeout);

        _parallelProcessor.subscribe(UniLists.of(Topics.IN_DN));
        _parallelProcessor.poll(ctx -> {
            var entry = ctx.getSingleConsumerRecord();
            var dn = entry.key();

            // Bounded the processing with the absolute processing timeout
            var processFuture = this.evaluateDN(dn)
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
     * Asynchronously evaluates a given domain name using the URLVoid website.
     *
     * @param dn The domain name to process.
     * @return A CompletableFuture that completes when the evaluation is done.
     */
    protected CompletableFuture<Void> evaluateDN(DNToProcess dn) {
        String requestUrl = URLVOID_BASE + dn.dn();

        return CompletableFuture.runAsync(() -> {
            try {
                Document document = Jsoup.connect(requestUrl).get();

                // Expect that Urlvoid has no data about this domain
                Integer cnt = null;

                String detectionCounts = parseDetectionCounts(document);

                if (detectionCounts != null) {
                    String[] parts = detectionCounts.split("/");
                    cnt = Integer.parseInt(parts[0].trim());
                }

                Logger.trace("Success: {}", dn);

                _producer.send(resultRecord(Topics.OUT_DN, dn,
                        successResult(new UrlvoidData(cnt))));
            }
            catch (IOException e) {
                Logger.debug("Cannot fetch");

                _producer.send(resultRecord(Topics.OUT_DN, dn,
                        errorResult(ResultCodes.CANNOT_FETCH, getName())));
            }
        });
    }

    /**
     * Parses the detection count from the URLVoid HTML report.
     *
     * @param document The Jsoup Document object representing the HTML page.
     * @return A String in the form of "x/y" representing detection counts, or null if not found.
     */
    private String parseDetectionCounts(Document document) {
        Element reportTable = document.select("table").first();
        if (reportTable != null) {
            Elements reportTableRows = reportTable.select("tr");

            if (reportTableRows.size() > 2) {
                Element detectionCountsSpan = reportTableRows.get(2).select("td").get(1)
                        .select("span").first();

                if (detectionCountsSpan != null) {
                    return detectionCountsSpan.text();
                }
            }
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
