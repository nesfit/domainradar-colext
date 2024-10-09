package cz.vut.fit.domainradar;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import cz.vut.fit.domainradar.models.results.Result;

import java.io.IOException;

public abstract class CommonDeserializer {
    private transient JsonFactory _jsonFactory;

    /**
     * Parses the status code only from a JSON-serialized {@link Result}.
     *
     * @param resultInput The bytes of a JSON-serialized Result.
     * @return The status code.
     */
    public int parseStatusCode(byte[] resultInput) {
        if (_jsonFactory == null) {
            _jsonFactory = new JsonFactory();
        }

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
