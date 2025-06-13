package cz.vut.fit.domainradar.flink.serialization;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import cz.vut.fit.domainradar.models.results.Result;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;

public abstract class CommonDeserializer {
    private transient JsonFactory _jsonFactory;

    public record StatusMeta(int statusCode, @Nullable String error, @Nullable String collector) {
    }

    /**
     * Parses the status code and error message only from a JSON-serialized collection {@link Result}.
     *
     * @param resultInput The bytes of a JSON-serialized collection {@link Result}.
     * @return A {@link StatusMeta} object containing the status code and error message.
     */
    public StatusMeta parseStatusMeta(byte[] resultInput, boolean includeCollector) {
        if (_jsonFactory == null) {
            _jsonFactory = new JsonFactory();
        }

        int statusCode = -1;
        String error = null, collector = null;
        var entriesLeft = includeCollector ? 3 : 2; // statusCode, error, collector (if included)

        try (JsonParser parser = _jsonFactory.createParser(resultInput)) {
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
                        statusCode = parser.getIntValue();
                        entriesLeft--;
                    } else if ("error".equals(fieldName)) {
                        if (parser.nextToken() == JsonToken.VALUE_NULL) {
                            error = null;
                        } else {
                            error = parser.getText();
                        }
                        entriesLeft--;
                    } else if (includeCollector && "collector".equals(fieldName)) {
                        parser.nextToken();
                        collector = parser.getText();
                        entriesLeft--;
                    }

                    // No need to parse further if we got all required fields
                    if (entriesLeft == 0) {
                        break;
                    }
                }
            }
        } catch (IOException e) {
            // TODO: Log?
            return new StatusMeta(statusCode, error, collector);
        }

        return new StatusMeta(statusCode, error, collector);
    }
}
