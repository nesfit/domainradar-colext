package cz.vut.fit.domainradar;

import cz.vut.fit.domainradar.flink.models.BaseKafkaDomainResult;
import cz.vut.fit.domainradar.flink.models.KafkaRecordMetadata;
import cz.vut.fit.domainradar.models.results.Result;
import cz.vut.fit.domainradar.serialization.JsonDeserializer;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaDomainResultDeserializer<D extends Result, T extends BaseKafkaDomainResult> implements KafkaRecordDeserializationSchema<T> {
    private transient Deserializer<String> _keyDeserializer;
    private transient Deserializer<D> _valueDeserializer;
    private transient Class<T> _resultClass;
    private transient Class<D> _dataClass;

    private final String _resultType;
    private final String _dataType;

    public KafkaDomainResultDeserializer(String kafkaResultType, String dataType) {
        _resultType = kafkaResultType;
        _dataType = dataType;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> consumerRecord, Collector<T> collector) {
        if (_resultClass == null) {
            try {
                _resultClass = (Class<T>) Class.forName(_resultType);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
        if (_dataClass == null) {
            try {
                _dataClass = (Class<D>) Class.forName(_dataType);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
        if (_keyDeserializer == null) {
            _keyDeserializer = new StringDeserializer();
        }
        if (_valueDeserializer == null) {
            var mapper = Common.makeMapper().build();
            _valueDeserializer = new JsonDeserializer<>(mapper, _dataClass);
        }

        String key = _keyDeserializer.deserialize(consumerRecord.topic(), consumerRecord.key());
        D value = _valueDeserializer.deserialize(consumerRecord.topic(), consumerRecord.value());

        var metadata = new KafkaRecordMetadata(consumerRecord.topic(), consumerRecord.partition(),
                consumerRecord.offset(), consumerRecord.timestamp(),
                value.statusCode(), value.lastAttempt().toEpochMilli());
        try {
            T result = _resultClass.getConstructor(String.class, _dataClass, KafkaRecordMetadata.class)
                    .newInstance(key, value, metadata);

            System.err.println(result);
            collector.collect(result);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public TypeInformation<T> getProducedType() {
        try {
            @SuppressWarnings("unchecked")
            var resultClass = (Class<T>) Class.forName(_resultType);
            return TypeInformation.of(resultClass);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
