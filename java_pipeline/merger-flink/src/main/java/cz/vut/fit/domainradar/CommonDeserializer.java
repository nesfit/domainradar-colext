package cz.vut.fit.domainradar;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import cz.vut.fit.domainradar.models.results.DNSResult;
import cz.vut.fit.domainradar.models.results.Result;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CommonDeserializer {
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

    /**
     * Parses the IP addresses only from a JSON-serialized {@link DNSResult}.
     *
     * @param dnsResultInput The bytes of a JSON-serialized DNSResult.
     * @return A non-null array of IP addresses.
     */
    public List<String> parseIPs(byte[] dnsResultInput) {
        if (_jsonFactory == null) {
            _jsonFactory = new JsonFactory();
        }

        try (JsonParser parser = _jsonFactory.createParser(dnsResultInput)) {
            // Start parsing the JSON
            while (!parser.isClosed()) {
                JsonToken token = parser.nextToken();

                // Check if the token is a field name
                if (JsonToken.FIELD_NAME.equals(token)) {
                    String fieldName = parser.currentName();

                    // If the field is "ips", get its value
                    if ("ips".equals(fieldName)) {
                        // Move to the next token which is the value of "ips"
                        token = parser.nextToken();

                        if (JsonToken.VALUE_NULL.equals(token)) {
                            // Handle null value
                            return List.of();
                        } else if (JsonToken.START_ARRAY.equals(token)) {
                            // Handle array of strings
                            var ips = new ArrayList<String>();

                            // Loop through the array
                            while (parser.nextToken() != JsonToken.END_ARRAY) {
                                // We expect each element in the array to be an object
                                var ip = extractIP(parser);
                                if (ip != null)
                                    ips.add(ip);
                            }

                            return ips;
                        }
                    }
                }
            }
        } catch (IOException e) {
            // TODO: Log?
            return List.of();
        }

        return List.of();
    }

    private static String extractIP(JsonParser parser) throws IOException {
        if (JsonToken.START_OBJECT.equals(parser.currentToken())) {
            String ip = null;

            // Parse the fields inside the object
            while (parser.nextToken() != JsonToken.END_OBJECT) {
                String objectFieldName = parser.currentName();

                // If the field is "ip", get its value
                if ("ip".equals(objectFieldName)) {
                    parser.nextToken(); // Move to the value of "ip"
                    ip = parser.getValueAsString();
                }
            }

            // Add the extracted IP to the list
            return ip;
        }

        return null;
    }
}
