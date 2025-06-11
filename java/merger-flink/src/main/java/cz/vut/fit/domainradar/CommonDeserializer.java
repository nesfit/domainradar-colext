package cz.vut.fit.domainradar;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import cz.vut.fit.domainradar.models.results.Result;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;

public abstract class CommonDeserializer {
    private transient JsonFactory _jsonFactory;

    public record StatusMeta(int statusCode, @Nullable String error) {
    }

    /**
     * Parses the status code and error message only from a JSON-serialized {@link Result}.
     *
     * @param resultInput The bytes of a JSON-serialized Result.
     * @return The status code.
     */
    public StatusMeta parseStatusMeta(byte[] resultInput) {
        if (_jsonFactory == null) {
            _jsonFactory = new JsonFactory();
        }

        int statusCode = -1;
        String error = null;
        boolean gotOne = false;

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
                        if (gotOne) break;
                        gotOne = true;
                    } else if ("error".equals(fieldName)) {
                        if (parser.nextToken() == JsonToken.VALUE_NULL) {
                            error = null;
                        } else {
                            error = parser.getText();
                        }
                        if (gotOne) break;
                        gotOne = true;
                    }
                }
            }
        } catch (IOException e) {
            // TODO: Log?
            return new StatusMeta(statusCode, error);
        }

        return new StatusMeta(statusCode, error);
    }
}
