package cz.vut.fit.domainradar.models.ip;

import org.jetbrains.annotations.Nullable;

/**
 * A record that represents a set of geolocation and autonomous system data about a specific IP address.
 *
 * @author Ondřej Ondryáš
 */
public record GeoIPData(@Nullable String continentCode,
                        @Nullable String countryCode,
                        @Nullable String region,
                        @Nullable String regionCode,
                        @Nullable String city,
                        @Nullable String postalCode,
                        @Nullable Double latitude,
                        @Nullable Double longitude,
                        @Nullable String timezone,
                        @Nullable Long registeredCountryGeoNameId,
                        @Nullable Long representedCountryGeoNameId,
                        @Nullable Long asn,
                        @Nullable String asnOrg,
                        @Nullable String networkAddress,
                        // Does not have to be long but the spec requires all integers to be 64bit
                        @Nullable Long prefixLength) {
}
