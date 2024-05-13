package cz.vut.fit.domainradar.standalone;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.vut.fit.domainradar.models.results.Result;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Properties;

public abstract class BiProducerStandaloneCollector<KIn, VIn, KOut1, VOut1 extends Result, KOut2, VOut2>
        extends BaseStandaloneCollector<KIn, VIn, KOut1, VOut1> {

    protected final KafkaProducer<KOut2, VOut2> _producer2;

    public BiProducerStandaloneCollector(@NotNull ObjectMapper jsonMapper, @NotNull String appName,
                                         @Nullable Properties properties,
                                         @NotNull Serde<KIn> keyInSerde,
                                         @NotNull Serde<KOut1> keyOut1Serde,
                                         @NotNull Serde<KOut2> keyOut2Serde,
                                         @NotNull Serde<VIn> valueInSerde,
                                         @NotNull Serde<VOut1> valueOut1Serde,
                                         @NotNull Serde<VOut2> valueOut2Serde) {
        super(jsonMapper, appName, properties, keyInSerde, keyOut1Serde, valueInSerde, valueOut1Serde);

        _producer2 = createProducer2(keyOut2Serde.serializer(), valueOut2Serde.serializer());
    }


    protected @NotNull KafkaProducer<KOut2, VOut2> createProducer2(@NotNull Serializer<KOut2> keySerializer,
                                                                @NotNull Serializer<VOut2> valueSerializer) {
        var properties = new Properties();
        properties.putAll(_properties);
        properties.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "producer-" + getName() + "-B");
        
        return new KafkaProducer<>(properties, keySerializer, valueSerializer);
    }

    @Override
    public void close() {
        super.close();
        _producer2.close(_closeTimeout);
    }
}
