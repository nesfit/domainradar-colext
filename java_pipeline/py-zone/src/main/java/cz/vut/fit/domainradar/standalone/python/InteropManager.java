package cz.vut.fit.domainradar.standalone.python;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.graalvm.polyglot.Context;
import org.graalvm.python.embedding.utils.GraalPyResources;
import org.graalvm.python.embedding.utils.VirtualFileSystem;

import java.io.Closeable;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Semaphore;

public class InteropManager
        implements Closeable {

    private final int _capacity;
    private volatile boolean _ended;

    private final Semaphore _endingSemaphore;
    private final BlockingQueue<Context> _pyContexts;

    public InteropManager(String targetModule, int capacity) {
        _capacity = capacity;
        _pyContexts = new ArrayBlockingQueue<>(_capacity, false);
        _ended = false;
        _endingSemaphore = new Semaphore(1, false);

        this.initPyContexts(targetModule);
    }

    private void initPyContexts(String targetModule) {
        final var vfs = VirtualFileSystem.newBuilder().allowHostIO(VirtualFileSystem.HostIO.READ).build();
        final var importCommand =
                "import " + targetModule + " as _target";

        for (var i = 0; i < _capacity; i++) {
            var context = GraalPyResources.contextBuilder(vfs)
                    .option("python.PythonHome", "")
                    .build();

            context.eval("python", importCommand);
            _pyContexts.add(context);
        }
    }

    public List<ProducerRecord<byte[], byte[]>> processBatch(List<ConsumerRecord<byte[], byte[]>> input) {
        if (_ended)
            return null;
        _endingSemaphore.acquireUninterruptibly();
        if (_ended) {
            _endingSemaphore.release();
            return null;
        }
        _endingSemaphore.release();

        // TODO
    }

    @Override
    public void close() throws IOException {
        _endingSemaphore.acquireUninterruptibly();
        _ended = true;
        _endingSemaphore.release();

        var contexts = new LinkedList<Context>();
        _pyContexts.drainTo(contexts);

        for (var context : contexts) {
            context.close(false);
        }
    }
}
