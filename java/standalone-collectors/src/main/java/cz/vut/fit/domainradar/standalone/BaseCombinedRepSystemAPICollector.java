package cz.vut.fit.domainradar.standalone;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.vut.fit.domainradar.models.DNToProcess;
import cz.vut.fit.domainradar.models.IPToProcess;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.jetbrains.annotations.NotNull;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Properties;

/**
 * An abstract base class for collectors that integrate both IP and domain name API collectors.
 *
 * @param <TData> The type of data returned by the collectors after processing the API response.
 * @author Matěj Čech
 */
public abstract class BaseCombinedRepSystemAPICollector<TData> implements CollectorInterface {
    protected final BaseIPRepSystemAPICollector<TData> ipCollector;
    protected final BaseDNRepSystemAPICollector<TData> dnCollector;

    public BaseCombinedRepSystemAPICollector(ObjectMapper jsonMapper, String appName, Properties properties,
                                             String authToken, String timeoutConfig, String timeoutDefault) {

        this.ipCollector = new BaseIPRepSystemAPICollector<TData>(jsonMapper, appName, properties, authToken,
                timeoutConfig, timeoutDefault) {
            @Override
            protected org.slf4j.Logger getLogger() {
                return BaseCombinedRepSystemAPICollector.this.getLogger();
            }

            @Override
            protected String getRequestUrl(IPToProcess ip) {
                return BaseCombinedRepSystemAPICollector.this.getRequestUrl(ip);
            }

            @Override
            protected String getAuthTokenHeaderName() {
                return BaseCombinedRepSystemAPICollector.this.getAuthTokenHeaderName();
            }

            @Override
            protected String getCollectorName() {
                return BaseCombinedRepSystemAPICollector.this.getCollectorName();
            }

            @Override
            protected TData mapResponseToData(JSONObject jsonResponse) {
                return BaseCombinedRepSystemAPICollector.this.mapIPResponseToData(jsonResponse);
            }

            @Override
            protected String getPOSTData(IPToProcess ip) {
                return BaseCombinedRepSystemAPICollector.this.getPOSTData(ip);
            }

            @Override
            protected String getUrlEncodedData(IPToProcess ip) {
                return BaseCombinedRepSystemAPICollector.this.getUrlEncodedData(ip);
            }
        };

        this.dnCollector = new BaseDNRepSystemAPICollector<TData>(jsonMapper, appName, properties, authToken,
                timeoutConfig, timeoutDefault) {
            @Override
            protected org.slf4j.Logger getLogger() {
                return BaseCombinedRepSystemAPICollector.this.getLogger();
            }

            @Override
            protected String getRequestUrl(DNToProcess dn) {
                return BaseCombinedRepSystemAPICollector.this.getRequestUrl(dn);
            }

            @Override
            protected String getAuthTokenHeaderName() {
                return BaseCombinedRepSystemAPICollector.this.getAuthTokenHeaderName();
            }

            @Override
            protected String getCollectorName() {
                return BaseCombinedRepSystemAPICollector.this.getCollectorName();
            }

            @Override
            protected TData mapResponseToData(JSONObject jsonResponse) {
                return BaseCombinedRepSystemAPICollector.this.mapDNResponseToData(jsonResponse);
            }

            @Override
            protected String getPOSTData(DNToProcess dn) {
                return BaseCombinedRepSystemAPICollector.this.getPOSTData(dn);
            }

            @Override
            protected String getUrlEncodedData(DNToProcess dn) {
                return BaseCombinedRepSystemAPICollector.this.getUrlEncodedData(dn);
            }
        };
    }

    /**
     * Gets the Logger defined by its child class.
     *
     * @return The logger instance used for logging messages from the specific collector.
     */
    protected abstract org.slf4j.Logger getLogger();

    /**
     * Builds the request URL for the given IP address.
     *
     * @param ip The IP address to process, which will be used to generate the request URL.
     * @return The constructed request URL for the given IP address.
     */
    protected abstract String getRequestUrl(IPToProcess ip);

    /**
     * Builds the request URL for the given domain name.
     *
     * @param dn The domain name to process, which will be used to generate the request URL.
     * @return The constructed request URL for the given domain name.
     */
    protected abstract String getRequestUrl(DNToProcess dn);

    /**
     * Gets the name of the HTTP header that should contain the authentication token for a given service the collector
     * uses.
     *
     * @return The name of the authentication token header or null if not required.
     */
    protected abstract String getAuthTokenHeaderName();

    /**
     * Gets the collector's name.
     *
     * @return The name of the collector.
     */
    protected abstract String getCollectorName();

    /**
     * Maps the JSON response about IP address from the API to the TData data type.
     *
     * @param jsonResponse The JSON response received from the API.
     * @return The mapped data in the TData data type.
     */
    protected abstract TData mapIPResponseToData(JSONObject jsonResponse);

    /**
     * Maps the JSON response about domain name from the API to the TData data type.
     *
     * @param jsonResponse The JSON response received from the API.
     * @return The mapped data in the TData data type.
     */
    protected abstract TData mapDNResponseToData(JSONObject jsonResponse);

    /**
     * Optionally provides POST data for the IP request.
     * By default, this method returns null, but it can be overridden by a subclass to provide POST data if needed.
     *
     * @param ip The IP address to be processed.
     * @return The POST data as a string, or null if no POST data is required.
     */
    protected String getPOSTData(IPToProcess ip) {
        return null;
    }

    /**
     * Optionally provides POST data for the DN request.
     * By default, this method returns null, but it can be overridden by a subclass to provide POST data if needed.
     *
     * @param dn The domain name to be processed.
     * @return The POST data as a string, or null if no POST data is required.
     */
    protected String getPOSTData(DNToProcess dn) {
        return null;
    }

    /**
     * Optionally provides URL-encoded data for the IP request.
     * By default, this method returns null, but it can be overridden by a subclass to provide URL-encoded data
     * for POST requests if needed.
     *
     * @param ip The IP address to be processed.
     * @return The URL-encoded data as a string, or null if no URL-encoded data is required.
     */
    protected String getUrlEncodedData(IPToProcess ip) {
        return null;
    }

    /**
     * Optionally provides URL-encoded data for the DN request.
     * By default, this method returns null, but it can be overridden by a subclass to provide URL-encoded data
     * for POST requests if needed.
     *
     * @param dn The domain name to be processed.
     * @return The URL-encoded data as a string, or null if no URL-encoded data is required.
     */
    protected String getUrlEncodedData(DNToProcess dn) {
        return null;
    }

    /**
     * Gets the name of the collector.
     *
     * @return The name of the collector.
     */
    @Override
    public @NotNull String getName() {
        return getCollectorName();
    }

    /**
     * Adds additional command-line options specific to the collector.
     *
     * @param options The options object to add the options to.
     */
    public void addOptions(@NotNull Options options) {
    }

    /**
     * Runs both collectors and executes the processing logic for consuming and gathering data from Kafka topics.
     *
     * @param cmd The parsed command line arguments.
     */
    public void run(CommandLine cmd) {
        ipCollector.run(cmd);
        dnCollector.run(cmd);
    }

    /**
     * Closes the collectors, stopping the parallel processor and the consumer.
     *
     * @throws IOException If an error occurs while closing the processor or the consumer.
     */
    @Override
    public void close() throws IOException {
        ipCollector.close();
        dnCollector.close();
    }
}
