package cz.vut.fit.domainradar.standalone.collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.vut.fit.domainradar.models.results.ZoneResult;
import cz.vut.fit.domainradar.serialization.JsonSerde;
import cz.vut.fit.domainradar.standalone.BaseStandaloneCollector;
import io.confluent.parallelconsumer.RecordContext;
import org.apache.commons.cli.CommandLine;
import org.apache.kafka.common.serialization.Serdes;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.LoggerFactory;
import pl.tlinkowski.unij.api.UniLists;

import java.util.Properties;
import java.util.Random;

public class TestCollector extends BaseStandaloneCollector<String, String, String, ZoneResult> {
    private static final org.slf4j.Logger Logger = LoggerFactory.getLogger(TestCollector.class);

    public TestCollector(@NotNull ObjectMapper jsonMapper, @NotNull String appName, @Nullable Properties properties) {
        super(jsonMapper, appName, properties,
                Serdes.String(), Serdes.String(), Serdes.String(), JsonSerde.of(jsonMapper, ZoneResult.class));
    }

    @Override
    public void run(CommandLine cmd) {
        buildProcessor(8);
        final var id = Integer.toHexString(hashCode());
        final var rnd = new Random();

        _parallelProcessor.subscribe(UniLists.of("test"));
        _parallelProcessor.poll(ctx -> {
            final var tid = Long.toHexString(Thread.currentThread().threadId());
            var msg = new StringBuilder();

            for(var tm : ctx.getByTopicPartitionMap().entrySet()) {
                var partition = tm.getKey().partition();
                msg.append("  -> Partition ");
                msg.append(partition);
                msg.append(":");
                var records = tm.getValue();
                for (RecordContext<String, String> r : records) {
                    msg.append(' ');
                    msg.append(r.value());
                    msg.append(" |");
                }
                msg.append('\n');
            }

            msg.deleteCharAt(msg.length() - 1);

            try {
                Thread.sleep(rnd.nextInt(100, 500));
            } catch (InterruptedException e) {
                // ignored
            }

            Logger.warn("Test[{}][T#{}]:\n{}", id, tid, msg.toString());
        });
    }

    @Override
    public @NotNull String getName() {
        return "test";
    }
}
