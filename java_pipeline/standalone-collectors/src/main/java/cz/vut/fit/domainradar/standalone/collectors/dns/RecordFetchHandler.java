package cz.vut.fit.domainradar.standalone.collectors.dns;

import cz.vut.fit.domainradar.models.dns.ZoneInfo;

import java.io.Closeable;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

public class RecordFetchHandler implements Closeable {

    private final LinkedBlockingQueue<ToProcessItem> _toProcess
            = new LinkedBlockingQueue<>();

    private final LinkedBlockingQueue<ProcessedItem> _processed
            = new LinkedBlockingQueue<>();

    private final ExecutorService _executorService;
    private final Thread[] _workers;

    public RecordFetchHandler(int workers, Properties properties) {
        _executorService = Executors.newVirtualThreadPerTaskExecutor();
        _workers = new Thread[workers + 1];

        for (var i = 0; i < workers; i++) {
            var worker = new RecordCollectorWorker(_toProcess, _processed, _executorService, properties);
            _workers[i] = new Thread(worker);
            _workers[i].start();
        }
    }

    public void submit(String domainName, ZoneInfo zoneInfo, String recordType) {
        _toProcess.offer(new ToProcessItem(domainName, zoneInfo, recordType));
    }

    @Override
    public void close() throws IOException {
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
    }
}
