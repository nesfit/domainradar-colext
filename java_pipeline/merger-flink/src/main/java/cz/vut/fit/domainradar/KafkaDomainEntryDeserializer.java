package cz.vut.fit.domainradar;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;

public class KafkaDomainEntryDeserializer implements KafkaRecordDeserializationSchema<KafkaDomainEntry> {
    private transient Deserializer<String> _keyDeserializer;
    private transient JsonFactory _jsonFactory;

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> consumerRecord, Collector<KafkaDomainEntry> collector) {
        if (_keyDeserializer == null) {
            _keyDeserializer = new StringDeserializer();
        }

        String key = _keyDeserializer.deserialize(consumerRecord.topic(), consumerRecord.key());
        int statusCode = parseStatusCode(consumerRecord.value());

        collector.collect(new KafkaDomainEntry(key, consumerRecord.value(), statusCode, consumerRecord.topic(),
                consumerRecord.partition(), consumerRecord.offset(), consumerRecord.timestamp()));
    }

    @Override
    public TypeInformation<KafkaDomainEntry> getProducedType() {
        return TypeInformation.of(KafkaDomainEntry.class);
    }

    /**
     * Parses the status code only from the input JSON bytes.
     *
     * @param input The input JSON data.
     * @return The status code.
     */
    private int parseStatusCode(byte[] input) {
        if (_jsonFactory == null) {
            _jsonFactory = new JsonFactory();
        }

        try (JsonParser parser = _jsonFactory.createParser(input)) {
            // Start parsing the JSON
            while (!parser.isClosed()) {
                JsonToken token = parser.nextToken();

                // Check if the token is a field name
                if (JsonToken.FIELD_NAME.equals(token)) {
                    String fieldName = parser.currentName();

                    // If the field is "statusCode", get its value
                    if ("statusCode".equals(fieldName)) {
                        // Move to the next token which is the value of "statusCode"
                        parser.nextToken();
                        return parser.getIntValue();
                    }
                }
            }
        } catch (IOException e) {
            // TODO: Log?
            return -1;
        }

        return -1;
    }
}
