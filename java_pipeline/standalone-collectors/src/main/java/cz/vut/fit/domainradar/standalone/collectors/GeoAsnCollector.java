package cz.vut.fit.domainradar.standalone.collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.net.InetAddresses;
import com.maxmind.db.CHMCache;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.model.AsnResponse;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.record.*;
import cz.vut.fit.domainradar.CollectorConfig;
import cz.vut.fit.domainradar.Topics;
import cz.vut.fit.domainradar.models.IPToProcess;
import cz.vut.fit.domainradar.models.ResultCodes;
import cz.vut.fit.domainradar.models.ip.GeoIPData;
import cz.vut.fit.domainradar.models.results.CommonIPResult;
import cz.vut.fit.domainradar.standalone.IPStandaloneCollector;
import org.apache.commons.cli.CommandLine;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jetbrains.annotations.NotNull;
import org.slf4j.LoggerFactory;
import pl.tlinkowski.unij.api.UniLists;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

public class GeoAsnCollector extends IPStandaloneCollector<GeoIPData> {
    private static final org.slf4j.Logger Logger = LoggerFactory.getLogger(GeoAsnCollector.class);
    public static final String NAME = "geo-asn";


    private final DatabaseReader _cityReader, _asnReader;

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
    }

    @Override
    public void run(CommandLine cmd) {
        buildProcessor(0);
        _parallelProcessor.subscribe(UniLists.of(Topics.IN_IP));
        _parallelProcessor.poll(ctx -> {
            var entry = ctx.getSingleConsumerRecord();

            var ip = entry.key();
            var request = entry.value();
            if (request == null || request.collectors() == null || request.collectors().contains(NAME))
                return;

            var result = this.evaluateIP(ip);
            _producer.send(new ProducerRecord<>(Topics.OUT_IP, ip, result));
        });
    }

    private CommonIPResult<GeoIPData> evaluateIP(IPToProcess ip) {
        try {
            var inetAddr = InetAddresses.forString(ip.ip());
            var cityOpt = _cityReader.tryCity(inetAddr);
            var asnOpt = _asnReader.tryAsn(inetAddr);

            if (cityOpt.isEmpty() && asnOpt.isEmpty()) {
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
                    network == null ? null : network.getPrefixLength()
            );

            return successResult(record);
        } catch (IllegalArgumentException e) {
            return errorResult(ResultCodes.INVALID_ADDRESS, e.getMessage());
        } catch (Exception e) {
            return errorResult(ResultCodes.OTHER_EXTERNAL_ERROR, e.getMessage());
        }
    }

    @Override
    public @NotNull String getName() {
        return NAME;
    }
}
