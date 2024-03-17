package cz.vut.fit.domainradar.pipeline.collectors;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import cz.vut.fit.domainradar.models.StringPair;
import cz.vut.fit.domainradar.models.ip.NERDData;
import cz.vut.fit.domainradar.models.results.CommonIPResult;
import cz.vut.fit.domainradar.pipeline.CommonResultIPCollector;
import cz.vut.fit.domainradar.serialization.JsonSerializer;
import cz.vut.fit.domainradar.serialization.StringPairSerde;
import io.confluent.parallelconsumer.vertx.VertxParallelStreamProcessor;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.RequestOptions;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import org.apache.kafka.streams.StreamsConfig;
import pl.tlinkowski.unij.api.UniLists;

import java.util.Properties;

public class NERDCollector implements CommonResultIPCollector<NERDData> {
    private final ObjectMapper _jsonMapper;
    private final Properties _properties;

    private Consumer<StringPair, String> _kafkaConsumer;
    private Producer<StringPair, CommonIPResult<NERDData>> _kafkaProducer;

    public NERDCollector(ObjectMapper jsonMapper, Properties properties) {
        _jsonMapper = jsonMapper;

        var appId = properties.getProperty(StreamsConfig.APPLICATION_ID_CONFIG);
        var groupId = appId + "-" + getName() + "-parallel-consumer-group";

        _properties = new Properties();
        _properties.putAll(properties);
        _properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        _properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    }

    private void setupParallelConsumer() {
        var stringPairSerde = StringPairSerde.build();

        _kafkaConsumer =
                new KafkaConsumer<>(_properties, stringPairSerde.deserializer(), Serdes.String().deserializer());
        _kafkaProducer =
                new KafkaProducer<>(_properties, stringPairSerde.serializer(), new JsonSerializer<>(_jsonMapper));

        var options = ParallelConsumerOptions.<StringPair, String>builder()
                .ordering(ParallelConsumerOptions.ProcessingOrder.KEY)
                .consumer(_kafkaConsumer)
                .maxConcurrency(64)
                .build();


        final var parallelSp = VertxParallelStreamProcessor.<StringPair, String>createEosStreamProcessor(options);
        parallelSp.subscribe(UniLists.of("to_process_IP"));
        parallelSp.vertxHttpRequest((client, ctx) -> {
            var url = "https://nerd.cesnet.cz/nerd/api/v1/ip/" + ctx.getSingleConsumerRecord().key().ip();
            return client.request(HttpMethod.GET, new RequestOptions()
                    .putHeader("Authorization", "token TOKENHERE")
                    .setAbsoluteURI(url));
        }, (unused) -> {
        }, (x) -> {
            CommonIPResult<NERDData> result;
            if (x.succeeded()) {
                var httpResult = x.result();
                if (x.result().statusCode() == 200) {
                    var resultData = httpResult.bodyAsJsonObject();
                    var rep = resultData.getDouble("rep", -1.0);
                    var fmpObj = resultData.getJsonObject("fmp");
                    var fmp = fmpObj == null ? -1.0 : fmpObj.getDouble("general", -1.0);
                    var data = new NERDData(rep, fmp);
                    result = successResult(data);
                } else {
                    result = errorResult(httpResult.statusMessage());
                }
            } else {
                result = errorResult(x.cause());
            }

            _kafkaProducer.send(new ProducerRecord<>("collected_IP_data", result));
        });
    }

    @Override
    public void use(StreamsBuilder builder) {
        setupParallelConsumer();
    }

    @Override
    public String getName() {
        return "COL_NERD";
    }

    @Override
    public String getCollectorName() {
        return "nerd";
    }
}