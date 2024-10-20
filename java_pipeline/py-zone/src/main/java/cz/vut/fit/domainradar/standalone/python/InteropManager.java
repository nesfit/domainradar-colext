package cz.vut.fit.domainradar.standalone.python;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Value;
import org.graalvm.python.embedding.utils.GraalPyResources;
import org.graalvm.python.embedding.utils.VirtualFileSystem;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class InteropManager
        implements Closeable {

    private static class ContextWrapper {
        public boolean ended = false;
        public Context context = null;
        public Value targetFunction = null;

        public ContextWrapper(Context context) {
            if (context == null) {
                this.ended = true;
            } else {
                this.context = context;
                this.targetFunction = context
                        .getBindings("python")
                        .getMember("collect");
            }
        }
    }

    private final int _capacity;
    private boolean _ended;
    private final BlockingQueue<ContextWrapper> _pyContexts;

    public InteropManager(String targetModule, int capacity) {
        _capacity = capacity;
        _pyContexts = new ArrayBlockingQueue<>(_capacity, false);
        this.initPyContexts(targetModule);
    }

    private void initPyContexts(String targetModule) {
        final var resourcesDir = "/home/ondryaso/Projects/domainradar/colext/java_pipeline/py-zone/python-env";
        final var importCommand =
                "import " + targetModule + " as _target\n" + """
                        import asyncio
                        def collect(input):
                            try:
                                return asyncio.run(_target.collect(input))
                            except Exception as e:
                                return None
                        """;

        for (var i = 0; i < _capacity; i++) {
            var context = GraalPyResources.contextBuilder(Path.of(resourcesDir))
                    .allowAllAccess(true)
                    .build();

            context.eval("python", importCommand);
            _pyContexts.add(new ContextWrapper(context));
        }
    }

    public List<ProducerRecord<byte[], byte[]>> processBatch(List<ConsumerRecord<byte[], byte[]>> input) {
        if (_ended)
            return null;

        ContextWrapper contextWrapper;
        try {
            contextWrapper = _pyContexts.take();
            if (contextWrapper.ended) {
                return null;
            }
        } catch (InterruptedException e) {
            return null;
        }

        try {
            final var collectFn = contextWrapper.targetFunction;
            final var result = collectFn.execute(input);

            if (result == null || result.isNull())
                return null;

            final var returnList = new ArrayList<ProducerRecord<byte[], byte[]>>();
            final var keysIterator = result.getHashKeysIterator();

            while(!keysIterator.hasIteratorNextElement()) {
                final var keyValue = keysIterator.getIteratorNextElement();
                final var topic = keyValue.asString();

                var listOfResults = result.getHashValue(keyValue);
                for (int i = 0; i < listOfResults.getArraySize(); i++) {
                    Value tuple = listOfResults.getArrayElement(i);
                    byte[] first = tuple.getArrayElement(0).as(byte[].class);
                    byte[] second = tuple.getArrayElement(1).as(byte[].class);
                    returnList.add(new ProducerRecord<>(topic, first, second));
                }
            }

            return returnList;
        } finally {
            // Return the context to the pool
            _pyContexts.add(contextWrapper);
        }
    }

    @Override
    public void close() throws IOException {
        _ended = true;

        var contexts = new LinkedList<ContextWrapper>();
        _pyContexts.drainTo(contexts);

        for (var context : contexts) {
            _pyContexts.add(new ContextWrapper(null));
            context.context.close(false);
        }
    }
}
