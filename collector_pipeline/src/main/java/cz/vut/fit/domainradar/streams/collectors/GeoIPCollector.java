package cz.vut.fit.domainradar.streams.collectors;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.maxmind.db.CHMCache;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.model.AsnResponse;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.record.*;
import cz.vut.fit.domainradar.CollectorConfig;
import cz.vut.fit.domainradar.Topics;
import cz.vut.fit.domainradar.models.ip.GeoIPData;
import cz.vut.fit.domainradar.models.results.CommonIPResult;
import cz.vut.fit.domainradar.streams.CommonResultIPCollector;
import cz.vut.fit.domainradar.models.ResultCodes;
import cz.vut.fit.domainradar.serialization.JsonSerde;
import cz.vut.fit.domainradar.serialization.StringPairSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Properties;

public class GeoIPCollector implements CommonResultIPCollector<GeoIPData> {
    private final ObjectMapper _jsonMapper;
    private final TypeReference<CommonIPResult<GeoIPData>> _resultTypeRef = new TypeReference<>() {
    };
    private final DatabaseReader _cityReader, _asnReader;

    private final CityResponse _emptyCity = new CityResponse(new City(), new Continent(), new Country(),
            new Location(), null, new Postal(), new Country(), new RepresentedCountry(), new ArrayList<>(0), null);

    private final AsnResponse _emptyAsn = new AsnResponse(0L, null, null, null);

    public GeoIPCollector(ObjectMapper jsonMapper, Properties properties) throws IOException {
        _jsonMapper = jsonMapper;

        var dbDir = new File(properties.getProperty(CollectorConfig.GEOIP_DIRECTORY_CONFIG));
        var asnDb = new File(dbDir, properties.getProperty(CollectorConfig.GEOIP_ASN_DB_NAME_CONFIG,
                CollectorConfig.GEOIP_ASN_DB_NAME_DEFAULT));
        var cityDb = new File(dbDir, properties.getProperty(CollectorConfig.GEOIP_CITY_DB_NAME_CONFIG,
                CollectorConfig.GEOIP_CITY_DB_NAME_DEFAULT));

        _asnReader = new DatabaseReader.Builder(asnDb).withCache(new CHMCache()).build();
        _cityReader = new DatabaseReader.Builder(cityDb).withCache(new CHMCache()).build();
    }

    @Override
    public void use(StreamsBuilder builder) {
        builder.stream(Topics.IN_IP, Consumed.with(StringPairSerde.build(), Serdes.Void()))
                .mapValues((ip, noValue) -> {
                    try {
                        var inetAddr = InetAddress.getByName(ip.ip());
                        var cityOpt = _cityReader.tryCity(inetAddr);
                        var asnOpt = _asnReader.tryAsn(inetAddr);
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
                    } catch (Exception e) {
                        // TODO
                        return errorResult(e.getMessage(), ResultCodes.OTHER_EXTERNAL_ERROR);
                    }

                }, namedOp("resolve"))
                .to(Topics.OUT_IP, Produced.with(StringPairSerde.build(), JsonSerde.of(_jsonMapper, _resultTypeRef)));
    }

    @Override
    public String getName() {
        return "COL_GEOIP";
    }

    @Override
    public String getCollectorName() {
        return "geo_asn";
    }
}