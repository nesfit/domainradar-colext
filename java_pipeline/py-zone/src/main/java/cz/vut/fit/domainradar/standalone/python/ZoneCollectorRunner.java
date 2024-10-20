package cz.vut.fit.domainradar.standalone.python;

import cz.vut.fit.domainradar.Topics;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class ZoneCollectorRunner {
    public static void main(String[] args) throws IOException {
        var manager = new InteropManager("zone_interop", 1);
        var test = List.of(new ConsumerRecord<>(Topics.IN_ZONE, 0, 0,
                "fit.vut.cz".getBytes(StandardCharsets.UTF_8),
                "".getBytes(StandardCharsets.UTF_8)));

        var result = manager.processBatch(test);
        for (var record : result) {
            System.out.println(new String(record.key(), StandardCharsets.UTF_8));
            System.out.println(new String(record.value(), StandardCharsets.UTF_8));
        }

        manager.close();
    }
}
