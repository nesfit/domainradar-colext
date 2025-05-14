package cz.vut.fit.domainradar.standalone;

import cz.vut.fit.domainradar.TriConsumer;
import cz.vut.fit.domainradar.models.ResultCodes;
import org.json.JSONObject;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpConnectTimeoutException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.function.Function;
import java.util.function.BiConsumer;
import java.util.concurrent.*;

/**
 * A base class responsible for interacting with the remote API.
 * This class abstracts the common logic for sending requests to an API and handling the responses,
 * enabling subclasses to define specific details such as request URL and response mapping.
 *
 * @param <TIn>   The input data type.
 * @param <TData> The data type that the response is mapped to after processing.
 * @author Matěj Čech
 */
public class RepSystemAPIClient<TIn, TData> {
    private final ExecutorService _executor;
    private final String _token;
    private final HttpClient _client;
    private final Duration _httpTimeout;
    private final boolean _disabled;

    public RepSystemAPIClient(String token, Duration timeout) {
        this._token = token;
        this._httpTimeout = timeout;
        this._disabled = this._token.isBlank() || this._token.equals("Bearer ");
        this._executor = Executors.newVirtualThreadPerTaskExecutor();

        // Create an HTTP client
        _client = HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.NEVER)
                .connectTimeout(_httpTimeout)
                .version(HttpClient.Version.HTTP_1_1)
                .executor(_executor)
                .build();
    }

    /**
     * Checks if the client is disabled based on the authentication token
     *
     * @return True if the client is disabled (token is blank or equals to "Bearer "), otherwise false
     */
    public boolean isDisabled() {
        return _disabled;
    }

    /**
     * Gets the executor service used to execute HTTP requests asynchronously.
     *
     * @return The ExecutorService instance used for handling tasks
     */
    public ExecutorService getExecutor() {
        return _executor;
    }

    /**
     * Gets the HTTP client used to send requests.
     *
     * @return The HttpClient instance used for HTTP requests.
     */
    public HttpClient getClient() {
        return _client;
    }

    /**
     * Executes an asynchronous HTTP request to a remote API and handles the response.
     *
     * @param input               The input data that will be sent in the request.
     * @param requestUrlProvider  A function that generates the request URL based on the input.
     * @param urlEncodedProvider  A function that generates URL-encoded data for the POST request, if applicable.
     * @param postDataProvider    A function that generates the JSON data for the POST request, if applicable.
     * @param authHeaderName      The name of the authorization header (or null if no auth is needed).
     * @param logger              A logger instance for logging errors and debug information.
     * @param onSuccess           A BiConsumer callback for handling successful responses.
     * @param onError             A BiConsumer callback for handling error responses.
     * @param responseMapper      A function that maps the JSON response body to the desired data type.
     * @param collectorName       The name of the collector.
     * @param processingTimeoutMs The timeout duration for processing the request in milliseconds.
     * @return A CompletableFuture representing the asynchronous task.
     */
    public CompletableFuture<Void> execute(
            TIn input,
            Function<TIn, String> requestUrlProvider,
            Function<TIn, String> urlEncodedProvider,
            Function<TIn, String> postDataProvider,
            String authHeaderName,
            org.slf4j.Logger logger,
            BiConsumer<TIn, TData> onSuccess,
            TriConsumer<TIn, Integer, String> onError,
            Function<JSONObject, TData> responseMapper,
            String collectorName,
            long processingTimeoutMs
    ) {
        if (_disabled) {
            onError.accept(input, ResultCodes.DISABLED, "Disabled");
            return CompletableFuture.completedFuture(null);
        }

        String url = requestUrlProvider.apply(input);

        if (url == null) {
            logger.debug("Discarding unsupported address");

            onError.accept(input, ResultCodes.UNSUPPORTED_ADDRESS, null);
            return CompletableFuture.completedFuture(null);
        }

        var requestBuilder = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(_httpTimeout)
                .header("Accept", "application/json");

        if (authHeaderName != null) {
            requestBuilder.header(authHeaderName, _token);
        }

        // Add POST urlencoded data, if required
        String urlEncodedData = urlEncodedProvider.apply(input);
        if (urlEncodedData != null) {
            requestBuilder.header("Content-Type", "application/x-www-form-urlencoded")
                    .POST(HttpRequest.BodyPublishers.ofString(urlEncodedData));
        }

        // Add POST data, if required
        String postData = postDataProvider.apply(input);
        if (postData != null) {
            requestBuilder.header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(postData));
        }

        var request = requestBuilder.build();

        // Send the HTTP request asynchronously and handle the response
        return _client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .orTimeout(processingTimeoutMs, TimeUnit.MILLISECONDS)
                .thenAccept(response -> {
                    if (response.statusCode() == 200) {
                        TData result = responseMapper.apply(new JSONObject(response.body()));
                        onSuccess.accept(input, result);
                    }
                    else if (response.statusCode() == 429) {
                        onError.accept(input, ResultCodes.RATE_LIMITED,
                                collectorName + " is rate limited");
                    }
                    else {
                        onError.accept(input, ResultCodes.CANNOT_FETCH,
                                collectorName + " response " + response.statusCode());
                    }
                })
                .exceptionally(e -> {
                    Throwable cause = e.getCause();
                    if (cause instanceof HttpConnectTimeoutException) {
                        logger.debug("Connection timeout");
                        onError.accept(input, ResultCodes.TIMEOUT,
                                "Connection timed out (%d ms)".formatted(_httpTimeout.toMillis()));
                    }
                    else if (cause instanceof IOException) {
                        logger.debug("I/O exception");
                        onError.accept(input, ResultCodes.INTERNAL_ERROR, cause.getMessage());
                    }
                    else {
                        logger.warn("Unexpected error", e);
                        onError.accept(input, ResultCodes.INTERNAL_ERROR, cause.getMessage());
                    }
                    return null;
                });
    }
}