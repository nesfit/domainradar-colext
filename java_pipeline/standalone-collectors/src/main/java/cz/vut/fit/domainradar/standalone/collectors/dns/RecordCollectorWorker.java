package cz.vut.fit.domainradar.standalone.collectors.dns;

import cz.vut.fit.domainradar.standalone.collectors.InternalDNSResolver;
import org.slf4j.LoggerFactory;
import org.xbill.DNS.TextParseException;

import java.net.UnknownHostException;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;

public class RecordCollectorWorker implements Runnable {
    private static final org.slf4j.Logger Logger = LoggerFactory.getLogger(RecordCollectorWorker.class);

    private final BlockingQueue<ToProcessItem> _toProcess;
    private final Queue<ProcessedItem> _processed;
    private final InternalDNSResolver _dnsResolver;

    public RecordCollectorWorker(BlockingQueue<ToProcessItem> toProcess,
                                 Queue<ProcessedItem> processed,
                                 ExecutorService executorService, Properties properties) {
        _toProcess = toProcess;
        _processed = processed;

        try {
            _dnsResolver = new InternalDNSResolver(executorService, properties);
        } catch (UnknownHostException e) {
            // TODO
            throw new RuntimeException(e);
        }
    }

    @Override
    public void run() {
        Logger.debug("RecordCollectorWorker started");

        while (true) {
            try {
                // Wait for the next item
                ToProcessItem item = _toProcess.take();
                // Process the item
                var processedItem = processItem(item);
                // Add the processed item to the queue
                _processed.offer(processedItem);
            } catch (InterruptedException e) {
                break;
            }
        }

        Logger.debug("RecordCollectorWorker stopped");
        Thread.currentThread().interrupt();
    }

    private ProcessedItem processItem(ToProcessItem item) {
        final String dn = item.domainName();
        final String recordType = item.recordType();

        Logger.trace("{} / {}: Collection started", dn, recordType);

        InternalDNSResolver.DNSScanner scanner;
        try {
            scanner = _dnsResolver.makeScanner(dn, item.zoneInfo());
        } catch (TextParseException e) {
            Logger.trace("{} / {}: TextParseException", dn, recordType);
            return new ProcessedItem(dn, recordType, null, -1, e.getMessage());
        }

        CompletionStage<?> resolveStage;
        switch (recordType) {
            case "A":
                resolveStage = scanner.resolveA();
                break;
            case "AAAA":
                resolveStage = scanner.resolveAAAA();
                break;
            case "CNAME":
                resolveStage = scanner.resolveCNAME();
                break;
            case "MX":
                resolveStage = scanner.resolveMX();
                break;
            case "NS":
                resolveStage = scanner.resolveNS();
                break;
            case "TXT":
                resolveStage = scanner.resolveTXT();
                break;
            default:
                return new ProcessedItem(dn, recordType, null, -1, "Invalid record type.");
        }

        var x = System.nanoTime();
        try {
            var result = (InternalDNSResolver.TTLTuple<?>) resolveStage.toCompletableFuture()
                    .orTimeout(_maxTimePerRecord, TimeUnit.MILLISECONDS).join();

            Logger.trace("{} / {}: Resolution finished in {} ms", dn, recordType, (System.nanoTime() - x) / 1_000_000);

            return new ProcessedItem(item.domainName(), item.recordType(), result.value(), result.ttl(), null);
        } catch (CompletionException e) {
            Logger.trace("{} / {}: Resolution exception in {} ms", dn, recordType, (System.nanoTime() - x) / 1_000_000);

            if (e.getCause() instanceof TimeoutException) {
                Logger.trace("{} / {}: Timeout", dn, recordType);
                return new ProcessedItem(dn, recordType, null, -1, "Timeout");
            } else {
                Logger.trace("{} / {}: Error", dn, recordType, e.getCause());
                return new ProcessedItem(dn, recordType, null, -1,
                        e.getCause().getClass().getName() + ": " + e.getCause().getMessage());
            }
        }
    }
}
