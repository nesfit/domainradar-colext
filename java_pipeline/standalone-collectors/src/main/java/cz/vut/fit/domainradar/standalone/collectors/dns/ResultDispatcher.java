package cz.vut.fit.domainradar.standalone.collectors.dns;

import cz.vut.fit.domainradar.Topics;
import cz.vut.fit.domainradar.models.IPToProcess;
import cz.vut.fit.domainradar.models.ResultCodes;
import cz.vut.fit.domainradar.models.dns.DNSData;
import cz.vut.fit.domainradar.models.results.DNSResult;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jetbrains.annotations.NotNull;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class ResultDispatcher implements Runnable {
    private static final org.slf4j.Logger Logger = LoggerFactory.getLogger(ResultDispatcher.class);

    private final BlockingQueue<ProcessedItem> _processedItems;
    private final KafkaProducer<String, DNSResult> _resultProducer;
    private final KafkaProducer<IPToProcess, Void> _ipResultProducer;
    private final KafkaProducer<String, String> _tlsRequestProducer;
    private final ConcurrentHashMap<String, DNSDataContainer> _inFlight;

    public ResultDispatcher(BlockingQueue<ProcessedItem> processedItems,
                            KafkaProducer<String, DNSResult> resultProducer,
                            KafkaProducer<IPToProcess, Void> ipRequestProducer,
                            KafkaProducer<String, String> tlsRequestProducer,
                            ConcurrentHashMap<String, DNSDataContainer> inFlight) {

        _processedItems = processedItems;
        _resultProducer = resultProducer;
        _ipResultProducer = ipRequestProducer;
        _tlsRequestProducer = tlsRequestProducer;
        _inFlight = inFlight;
    }

    @Override
    public void run() {
        Logger.debug("ResultDispatcher started.");

        while (true) {
            try {
                // Wait for the next item
                ProcessedItem item = _processedItems.take();

                // Process the item
                Logger.trace("Handling processed {} from {}", item.recordType(), item.domainName());
                var result = handleItem(item);

                // If all wanted data has been collected (all bits are 0), dispatch the result
                if (result == 0) {
                    Logger.trace("Collected all record types for {}", item.domainName());

                    // All data is present
                    var container = _inFlight.remove(item.domainName());
                    if (container == null) {
                        Logger.warn("Received a result for a domain that is not in flight: {}", item.domainName());
                        continue;
                    }

                    if (container.clearStalledTask != null) {
                        container.clearStalledTask.cancel();
                    }

                    this.sendResult(item.domainName(), container);

                    Logger.trace("Finished {}", item.domainName());
                }
            } catch (InterruptedException e) {
                break;
            }
        }

        Logger.debug("ResultDispatcher stopped.");
        Thread.currentThread().interrupt();
    }

    private static DNSResult errorResult(int code, @NotNull String message) {
        return new DNSResult(code, message, Instant.now(), null, null);
    }

    public void dispatchOnTimeout(String domainName) {
        // TODO: Send if something has been collected
        _inFlight.remove(domainName);
        _resultProducer.send(new ProducerRecord<>(Topics.OUT_DNS, domainName,
                errorResult(ResultCodes.CANNOT_FETCH,
                        "DNS scan took too long")));
    }

    @SuppressWarnings("unchecked")
    private int handleItem(ProcessedItem item) {
        var container = _inFlight.get(item.domainName());
        if (container == null) {
            Logger.warn("Received a result for a domain that is not in flight: {}", item.domainName());
            return -1;
        }

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

        if (item.error() != null) {
            container.recordCollectionErrors.put(item.recordType(), item.error());
        }

        return container.missing;
    }

    private static <T> Stream<T> streamIfNotNull(Collection<T> collection) {
        return collection == null ? Stream.empty() : collection.stream();
    }

    private Set<DNSResult.IPFromRecord> getIPsToProcess(DNSDataContainer data) {
        if (data.typesToProcessIPsFrom == null || data.typesToProcessIPsFrom.isEmpty())
            return Set.of();

        var ret = new HashSet<DNSResult.IPFromRecord>();
        for (var type : data.typesToProcessIPsFrom) {
            var ips = switch (type) {
                case "A" -> streamIfNotNull(data.A);
                case "AAAA" -> streamIfNotNull(data.AAAA);
                case "CNAME" -> data.CNAME == null
                        ? Stream.<String>empty()
                        : streamIfNotNull(data.CNAME.relatedIps());
                case "MX" -> data.MX == null
                        ? Stream.<String>empty()
                        : data.MX.stream().flatMap(x -> streamIfNotNull(x.relatedIps()));
                case "NS" -> data.NS == null
                        ? Stream.<String>empty()
                        : data.NS.stream().flatMap(x -> streamIfNotNull(x.relatedIps()));
                default -> Stream.<String>empty();
            };

            ret.addAll(ips.map(ip -> new DNSResult.IPFromRecord(ip, type)).collect(Collectors.toSet()));
        }

        return ret;
    }

    private void sendResult(String domainName, DNSDataContainer container) {
        var ttlValues = Map.of("A", container.ttlA, "AAAA", container.ttlAAAA, "CNAME", container.ttlCNAME,
                "MX", container.ttlMX, "NS", container.ttlNS, "TXT", container.ttlTXT);

        var dnsData = new DNSData(ttlValues, container.A, container.AAAA, container.CNAME,
                container.MX, container.NS, container.TXT, container.recordCollectionErrors.isEmpty() ? null : container.recordCollectionErrors);

        var ips = getIPsToProcess(container);
        var ipForTLS = getIPForTLS(dnsData);

        var result = new DNSResult(0, null, Instant.now(), dnsData, ips);

        Logger.trace("Producing DNS result");
        _resultProducer.send(new ProducerRecord<>(Topics.OUT_DNS, domainName, result));

        if (ipForTLS != null) {
            Logger.trace("Producing TLS request");
            _tlsRequestProducer.send(new ProducerRecord<>(Topics.IN_TLS, domainName, ipForTLS));
        } else {
            Logger.trace("No IP found for a TLS request");
        }

        Logger.trace("Producing IP requests");
        for (var ip : ips) {
            _ipResultProducer.send(new ProducerRecord<>(Topics.IN_IP, new IPToProcess(domainName, ip.ip()), null));
        }
    }

    private String getIPForTLS(DNSData dnsData) {
        String targetIp;

        // TODO: handle empty sets
        if (dnsData.CNAME() != null && dnsData.CNAME().relatedIps() != null && !dnsData.CNAME().relatedIps().isEmpty()) {
            targetIp = dnsData.CNAME().relatedIps().getFirst();
        } else if (dnsData.A() != null) {
            targetIp = dnsData.A().iterator().next();
        } else if (dnsData.AAAA() != null) {
            targetIp = dnsData.AAAA().iterator().next();
        } else {
            targetIp = null;
        }

        return targetIp;
    }
}
