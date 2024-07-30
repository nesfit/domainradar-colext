package cz.vut.fit.domainradar.standalone.collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.net.InetAddresses;
import com.maxmind.db.CHMCache;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.model.AsnResponse;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.record.*;
import cz.vut.fit.domainradar.CollectorConfig;
import cz.vut.fit.domainradar.Common;
import cz.vut.fit.domainradar.Topics;
import cz.vut.fit.domainradar.models.IPToProcess;
import cz.vut.fit.domainradar.models.ResultCodes;
import cz.vut.fit.domainradar.models.ip.GeoIPData;
import cz.vut.fit.domainradar.models.results.CommonIPResult;
import cz.vut.fit.domainradar.standalone.IPStandaloneCollector;
import org.apache.commons.cli.CommandLine;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jetbrains.annotations.NotNull;
import pl.tlinkowski.unij.api.UniLists;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

/**
 * A collector that processes GeoIP and ASN data for IP addresses.
 *
 * @author Ondřej Ondryáš
 */
public class GeoAsnCollector extends IPStandaloneCollector<GeoIPData> {
    public static final String NAME = "geo-asn";
    public static final String COMPONENT_NAME = "collector-geoip"; // weird exception
    private static final org.slf4j.Logger Logger = Common.getComponentLogger(GeoAsnCollector.class);

    private final DatabaseReader _cityReader, _asnReader;

    // Empty responses to avoid null checks
    private final CityResponse _emptyCity = new CityResponse(new City(), new Continent(), new Country(),
            new Location(), null, new Postal(), new Country(), new RepresentedCountry(), new ArrayList<>(0), null);
    private final AsnResponse _emptyAsn = new AsnResponse(0L, null, null, null);

    public GeoAsnCollector(@NotNull ObjectMapper jsonMapper, @NotNull String appName, @NotNull Properties properties) throws IOException {
        super(jsonMapper, appName, properties);

        var dbDir = new File(properties.getProperty(CollectorConfig.GEOIP_DIRECTORY_CONFIG,
                CollectorConfig.GEOIP_DIRECTORY_DEFAULT));
        var asnDb = new File(dbDir, properties.getProperty(CollectorConfig.GEOIP_ASN_DB_NAME_CONFIG,
                CollectorConfig.GEOIP_ASN_DB_NAME_DEFAULT));
        var cityDb = new File(dbDir, properties.getProperty(CollectorConfig.GEOIP_CITY_DB_NAME_CONFIG,
                CollectorConfig.GEOIP_CITY_DB_NAME_DEFAULT));

        _asnReader = new DatabaseReader.Builder(asnDb).withCache(new CHMCache()).build();
        _cityReader = new DatabaseReader.Builder(cityDb).withCache(new CHMCache()).build();

        Logger.info("GeoIP City+ASN databases loaded");
    }

    @Override
    public void run(CommandLine cmd) {
        buildProcessor(0);
        _parallelProcessor.subscribe(UniLists.of(Topics.IN_IP));
        _parallelProcessor.poll(ctx -> {
            var entry = ctx.getSingleConsumerRecord();

            var ip = entry.key();
            var request = entry.value();
            if (request != null && request.collectors() != null && !request.collectors().contains(NAME))
                return;

            var result = this.evaluateIP(ip);
            _producer.send(new ProducerRecord<>(Topics.OUT_IP, ip, result));
        });
    }

    private CommonIPResult<GeoIPData> evaluateIP(IPToProcess ip) {
        try {
            Logger.trace("Processing IP: {}", ip);
            var inetAddr = InetAddresses.forString(ip.ip());
            var cityOpt = _cityReader.tryCity(inetAddr);
            var asnOpt = _asnReader.tryAsn(inetAddr);

            if (cityOpt.isEmpty() && asnOpt.isEmpty()) {
                Logger.debug("No City or ASN data found for {}", ip.ip());
                return errorResult(ResultCodes.NOT_FOUND, "No GeoIP data found");
            }

            var city = cityOpt.orElse(_emptyCity);
            var asn = asnOpt.orElse(_emptyAsn);

            var region = city.getLeastSpecificSubdivision();
            var network = asn.getNetwork();

            var record = new GeoIPData(
                    city.getContinent().getCode(),
                    city.getCountry().getIsoCode(),
                    region.getIsoCode(),
                    region.getName(),
                    city.getCity().getName(),
                    city.getPostal().getCode(),
                    city.getLocation().getLatitude(),
                    city.getLocation().getLongitude(),
                    city.getLocation().getTimeZone(),
                    city.getRegisteredCountry().getGeoNameId(),
                    city.getRepresentedCountry().getGeoNameId(),
                    asn.getAutonomousSystemNumber(),
                    asn.getAutonomousSystemOrganization(),
                    network == null ? null : network.getNetworkAddress().toString(),
                    network == null ? null : (long) network.getPrefixLength()
            );

            Logger.trace("Success: {}", ip);
            return successResult(record);
        } catch (IllegalArgumentException e) {
            Logger.debug("Invalid IP address: {}", ip.ip());
            return errorResult(ResultCodes.INVALID_ADDRESS, e.getMessage());
        } catch (Exception e) {
            // Should not happen
            Logger.error("Error while reading GeoIP data for {}", ip, e);
            return errorResult(ResultCodes.CANNOT_FETCH, e.getMessage());
        }
    }

    @Override
    public @NotNull String getName() {
        return NAME;
    }
}
