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

public abstract class TriProducerStandaloneCollector<KIn, VIn, KOut1, VOut1 extends Result, KOut2, VOut2, KOut3, VOut3>
        extends BiProducerStandaloneCollector<KIn, VIn, KOut1, VOut1, KOut2, VOut2> {

    protected final KafkaProducer<KOut3, VOut3> _producer3;

    public TriProducerStandaloneCollector(@NotNull ObjectMapper jsonMapper, @NotNull String appName,
                                          @Nullable Properties properties,
                                          @NotNull Serde<KIn> keyInSerde,
                                          @NotNull Serde<KOut1> keyOut1Serde,
                                          @NotNull Serde<KOut2> keyOut2Serde,
                                          @NotNull Serde<KOut3> keyOut3Serde,
                                          @NotNull Serde<VIn> valueInSerde,
                                          @NotNull Serde<VOut1> valueOut1Serde,
                                          @NotNull Serde<VOut2> valueOut2Serde,
                                          @NotNull Serde<VOut3> valueOut3Serde) {
        super(jsonMapper, appName, properties, keyInSerde, keyOut1Serde, keyOut2Serde, valueInSerde, valueOut1Serde,
                valueOut2Serde);

        _producer3 = createProducer3(keyOut3Serde.serializer(), valueOut3Serde.serializer());
    }


    protected @NotNull KafkaProducer<KOut3, VOut3> createProducer3(@NotNull Serializer<KOut3> keySerializer,
                                                                   @NotNull Serializer<VOut3> valueSerializer) {
        var properties = new Properties();
        properties.putAll(_properties);
        properties.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "producer-" + getName() + "-C");
                                                                    
        return new KafkaProducer<>(properties, keySerializer, valueSerializer);
    }

    @Override
    public void close() {
        super.close();
        _producer3.close(_closeTimeout);
    }
}
