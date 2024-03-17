package cz.vut.fit.domainradar.pipeline.collectors;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import cz.vut.fit.domainradar.models.StringPair;
import cz.vut.fit.domainradar.models.ip.NERDData;
import cz.vut.fit.domainradar.models.results.CommonIPResult;
import cz.vut.fit.domainradar.pipeline.IPCollector;
import cz.vut.fit.domainradar.pipeline.PipelineComponent;
import cz.vut.fit.domainradar.serialization.JsonSerde;
import cz.vut.fit.domainradar.serialization.JsonSerializer;
import cz.vut.fit.domainradar.serialization.StringPairSerde;
import io.confluent.parallelconsumer.vertx.JStreamVertxParallelStreamProcessor;
import io.confluent.parallelconsumer.vertx.VertxParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.vertx.VertxParallelStreamProcessor;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.RequestOptions;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelStreamProcessor;
import pl.tlinkowski.unij.api.UniLists;

import java.time.Instant;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

public class NERDCollector implements IPCollector<CommonIPResult<NERDData>> {
    private final ObjectMapper _jsonMapper;
    private final TypeReference<CommonIPResult<NERDData>> _resultTypeRef = new TypeReference<>() {
    };

    public NERDCollector(ObjectMapper jsonMapper) {
        _jsonMapper = jsonMapper;
    }

    private void setupParallelConsumer(Properties properties) {
        var stringPairSerde = StringPairSerde.build();

        Consumer<StringPair, String> kafkaConsumer =
                new KafkaConsumer<>(properties, stringPairSerde.deserializer(), Serdes.String().deserializer());
        Producer<StringPair, CommonIPResult<NERDData>> kafkaProducer =
                new KafkaProducer<>(properties, stringPairSerde.serializer(), new JsonSerializer<>(_jsonMapper));

        var options = ParallelConsumerOptions.<StringPair, String>builder()
                .ordering(ParallelConsumerOptions.ProcessingOrder.KEY)
                .consumer(kafkaConsumer)
                .maxConcurrency(64)
                //.batchSize(5)
                .build();


        final var parallelSp = VertxParallelStreamProcessor.<StringPair, String>createEosStreamProcessor(options);
        parallelSp.subscribe(UniLists.of("to_process_IP"));
        parallelSp.vertxHttpRequest((client, ctx) -> {
            // var ri = new VertxParallelEoSStreamProcessor.RequestInfo("nerd.cesnet.cz", 443,
            // "/nerd/api/v1/ip/" + ctx.getSingleConsumerRecord().key().ip(), Map.of());
            var url = "https://nerd.cesnet.cz/nerd/api/v1/ip/" + ctx.getSingleConsumerRecord().key().ip();
            return client.request(HttpMethod.GET, new RequestOptions()
                    .putHeader("Authorization", "token TOKENHERE")
                    .setURI(url));
        }, (x) -> {
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
                    result = new CommonIPResult<>(true, null, Instant.now(), getCollectorName(),
                            data);
                } else {
                    result = errorResult(httpResult.statusMessage(), CommonIPResult.class);
                }
            } else {
                result = errorResult(x.cause(), CommonIPResult.class);
            }

            kafkaProducer.send(new ProducerRecord<>("collected_IP_data", result));
        });
    }

    @Override
    public void addTo(StreamsBuilder builder) {
        /*
        builder.stream("to_process_IP", Consumed.with(StringPairSerde.build(), Serdes.Void()))
                .map((ip, noValue) -> {

                    return KeyValue.pair(ip, new CommonIPResult<>(true, null, Instant.now(),
                            "nerd", new NERDData(rnd.nextDouble())));
                })
                .to("collected_IP_data", Produced.with(StringPairSerde.build(), JsonSerde.of(_jsonMapper, _resultTypeRef)));
        */
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