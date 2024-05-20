package cz.vut.fit.domainradar.standalone.collectors.dns;

import cz.vut.fit.domainradar.standalone.collectors.InternalDNSResolver;
import org.xbill.DNS.TextParseException;

import java.net.UnknownHostException;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;

public class RecordCollectorWorker implements Runnable {
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
        while (true) {
            try {
                // Wait for the next item
                ToProcessItem item = _toProcess.take();
                // Process the item
                var processedItem = processItem(item);
                // Add the processed item to the queue
                _processed.offer(processedItem);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private ProcessedItem processItem(ToProcessItem item) {
        InternalDNSResolver.DNSScanner scanner;
        try {
            scanner = _dnsResolver.makeScanner(item.domainName(), item.zoneInfo());
        } catch (TextParseException e) {
            // TODO
            return null;
        }

        CompletionStage<?> resolveStage;
        switch (item.recordType()) {
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
                return null;
        }

        try {
            var result = (InternalDNSResolver.TTLTuple<?>) resolveStage.toCompletableFuture().join();
            if (result == null)
                return null; // TODO

            return new ProcessedItem(item.domainName(), item.recordType(), result.value(), result.ttl());
        } catch (CompletionException e) {
            // TODO
            return null;
        }
    }
}
