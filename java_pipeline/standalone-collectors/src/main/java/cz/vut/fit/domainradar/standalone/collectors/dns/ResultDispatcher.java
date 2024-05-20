package cz.vut.fit.domainradar.standalone.collectors.dns;

import cz.vut.fit.domainradar.models.IPToProcess;
import cz.vut.fit.domainradar.models.dns.DNSData;
import cz.vut.fit.domainradar.models.results.DNSResult;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

public class ResultDispatcher implements Runnable {
    private static class DNSDataContainer {
        @Nullable Set<String> A;
        @Nullable Set<String> AAAA;
        @Nullable DNSData.CNAMERecord CNAME;
        @Nullable List<DNSData.MXRecord> MX;
        @Nullable List<DNSData.NSRecord> NS;
        @Nullable List<String> TXT;
        long ttlA, ttlAAAA, ttlCNAME, ttlMX, ttlNS, ttlTXT;
        byte missing = 0b111111;
    }

    private final BlockingQueue<ProcessedItem> _processedItems;
    private final KafkaProducer<String, DNSResult> _resultProducer;
    private final KafkaProducer<String, IPToProcess> _ipResultProducer;

    private final ConcurrentHashMap<String, DNSDataContainer> _data;

    public ResultDispatcher(BlockingQueue<ProcessedItem> processedItems,
                            KafkaProducer<String, DNSResult> resultProducer,
                            KafkaProducer<String, IPToProcess> ipResultProducer) {

        _processedItems = processedItems;
        _resultProducer = resultProducer;
        _ipResultProducer = ipResultProducer;
        _data = new ConcurrentHashMap<>();
    }

    @Override
    public void run() {
        while (true) {
            try {
                // Wait for the next item
                ProcessedItem item = _processedItems.take();
                // Process the item
                handleItem(item);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void handleItem(ProcessedItem item) {
        var container = _data.computeIfAbsent(item.domainName(), unused -> new DNSDataContainer());
        switch (item.recordType()) {
            case "A" -> {
                container.A = (Set<String>) item.value();
                container.ttlA = item.ttl();
                container.missing &= 0b011111;
            }
            case "AAAA" -> {
                container.AAAA = (Set<String>) item.value();
                container.ttlAAAA = item.ttl();
                container.missing &= 0b101111;
            }
            case "CNAME" -> {
                container.CNAME = (DNSData.CNAMERecord) item.value();
                container.ttlCNAME = item.ttl();
                container.missing &= 0b110111;
            }
            case "MX" -> {
                container.MX = (List<DNSData.MXRecord>) item.value();
                container.ttlMX = item.ttl();
                container.missing &= 0b111011;
            }
            case "NS" -> {
                container.NS = (List<DNSData.NSRecord>) item.value();
                container.ttlNS = item.ttl();
                container.missing &= 0b111101;
            }
            case "TXT" -> {
                container.TXT = (List<String>) item.value();
                container.ttlTXT = item.ttl();
                container.missing &= 0b111110;
            }
        }
    }

}
