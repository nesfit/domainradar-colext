package cz.vut.fit.domainradar.standalone.collectors.dns;

import cz.vut.fit.domainradar.CollectorConfig;
import cz.vut.fit.domainradar.models.IPToProcess;
import cz.vut.fit.domainradar.models.requests.DNSProcessRequest;
import cz.vut.fit.domainradar.models.results.DNSResult;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

public class RecordFetchHandler implements Closeable {
    private static final org.slf4j.Logger Logger = LoggerFactory.getLogger(RecordFetchHandler.class);

    private final LinkedBlockingQueue<ToProcessItem> _toProcess
            = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<ProcessedItem> _processed
            = new LinkedBlockingQueue<>();
    private final ConcurrentHashMap<String, DNSDataContainer> _inFlight
            = new ConcurrentHashMap<>();

    private final ExecutorService _executorService;
    private final ResultDispatcher _resultDispatcher;
    private final Thread[] _workers;
    private final Timer _clearStalledTimer;
    private final Properties _properties;
    private final int _defaultMask;
    private final long _stalledTimeout;

    private final List<String> _typesToCollect, _typesToProcessIPsFrom;

    public RecordFetchHandler(Properties properties,
                              KafkaProducer<String, DNSResult> resultProducer,
                              KafkaProducer<IPToProcess, Void> ipResultProducer,
                              KafkaProducer<String, String> tlsRequestProducer) {
        _properties = properties;

        _stalledTimeout = Long.parseLong(properties.getProperty(
                CollectorConfig.DNS_STALLED_TIMEOUT_MS_CONFIG, CollectorConfig.DNS_STALLED_TIMEOUT_MS_DEFAULT));

        _typesToCollect = this.parseConfig(CollectorConfig.DNS_DEFAULT_RECORD_TYPES_TO_COLLECT_CONFIG,
                CollectorConfig.DNS_DEFAULT_RECORD_TYPES_TO_COLLECT_DEFAULT);
        if (_typesToCollect == null) {
            throw new RuntimeException("No record types to collect configured");
        }

        _typesToProcessIPsFrom = this.parseConfig(CollectorConfig.DNS_DEFAULT_TYPES_TO_COLLECT_IPS_FROM_CONFIG,
                CollectorConfig.DNS_DEFAULT_TYPES_TO_COLLECT_IPS_FROM_DEFAULT);

        _defaultMask = makeToProcessMask(_typesToCollect);
        var workers = Integer.parseInt(properties.getProperty(
                CollectorConfig.DNS_WORKERS_CONFIG, CollectorConfig.DNS_WORKERS_DEFAULT));

        _executorService = Executors.newVirtualThreadPerTaskExecutor();
        _workers = new Thread[workers + 1];

        for (var i = 0; i < workers; i++) {
            var worker = new RecordCollectorWorker(_toProcess, _processed, _executorService, properties);
            _workers[i] = new Thread(worker);
            _workers[i].setName("DNS-RecordCollectorWorker-" + (i - 1));
            _workers[i].start();
        }

        _resultDispatcher = new ResultDispatcher(_processed, resultProducer, ipResultProducer,
                tlsRequestProducer, _inFlight);

        _workers[workers] = new Thread(_resultDispatcher);
        _workers[workers].setName("DNS-ResultDispatcher");
        _workers[workers].start();

        _clearStalledTimer = new Timer("DNS-ClearStalledInFlightDomains", false);

        Logger.info("DNS RecordFetchHandler started");
    }

    public void submit(String domainName, DNSProcessRequest processRequest) {
        int mask;
        List<String> typesToCollect;
        if (processRequest.typesToCollect() == null || processRequest.typesToCollect().isEmpty()) {
            mask = _defaultMask;
            typesToCollect = _typesToCollect;
        } else {
            mask = makeToProcessMask(processRequest.typesToCollect());
            typesToCollect = processRequest.typesToCollect();
        }

        // Add the domain name into the "in-flight" list.
        // It is highly improbable that a name would be submitted twice in a short time, but in case it did,
        // this will create the record atomically.
        var inFlightEntry = _inFlight.computeIfAbsent(domainName, unused -> {
            Logger.trace("Adding new DNS scan entry {}, to collect: {}", domainName, Integer.toHexString(mask));
            return new DNSDataContainer(mask,
                    Objects.requireNonNullElse(processRequest.typesToProcessIPsFrom(), _typesToProcessIPsFrom));
        });

        // This is a potential race, but it's not a problem if we schedule the task twice.
        // Better than synchronising inside the computeIfAbsent.
        if (inFlightEntry.clearStalledTask == null) {
            var task = new RemoveDomainTimerTask(domainName);
            _clearStalledTimer.schedule(task, _stalledTimeout);
            inFlightEntry.clearStalledTask = task;
        }

        // Insert the resolution requests into the "to process" queue.
        for (var recordType : typesToCollect) {
            _toProcess.offer(new ToProcessItem(domainName, processRequest.zoneInfo(), recordType));
        }
    }

    private class RemoveDomainTimerTask extends TimerTask {
        private static final org.slf4j.Logger Logger = LoggerFactory.getLogger(RemoveDomainTimerTask.class);
        private final String _domainName;

        public RemoveDomainTimerTask(String domainName) {
            this._domainName = domainName;
        }

        @Override
        public void run() {
            Logger.debug("Removing stalled {}", _domainName);
            _resultDispatcher.dispatchOnTimeout(_domainName);
        }
    }

    private static int makeToProcessMask(List<String> toProcess) {
        var mask = 0;
        for (var type : toProcess) {
            switch (type) {
                case "A":
                    mask |= RecordTypeFlags.A;
                    break;
                case "AAAA":
                    mask |= RecordTypeFlags.AAAA;
                    break;
                case "CNAME":
                    mask |= RecordTypeFlags.CNAME;
                    break;
                case "NS":
                    mask |= RecordTypeFlags.NS;
                    break;
                case "MX":
                    mask |= RecordTypeFlags.MX;
                    break;
                case "TXT":
                    mask |= RecordTypeFlags.TXT;
                    break;
            }
        }
        return mask;
    }

    private List<String> parseConfig(String configKey, String defaultValue) {
        var config = _properties.getProperty(configKey, defaultValue);

        if (config.isEmpty())
            return null;

        return Arrays.asList(config.split(","));
    }

    @Override
    public void close() {
        Logger.info("Closing the RecordFetchHandler");
        for (var worker : _workers) {
            worker.interrupt();

            // TODO
            try {
                worker.join(500);
            } catch (InterruptedException e) {
                // ignored
            }
        }
        _executorService.close();

        for (var dn : _inFlight.keySet()) {
            Logger.info("Not processed: {}", dn);
        }
    }
}
