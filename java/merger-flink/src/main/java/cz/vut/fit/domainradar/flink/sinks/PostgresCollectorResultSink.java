package cz.vut.fit.domainradar.flink.sinks;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import cz.vut.fit.domainradar.*;
import cz.vut.fit.domainradar.models.ip.GeoIPData;
import cz.vut.fit.domainradar.models.ip.NERDData;
import cz.vut.fit.domainradar.models.results.CommonIPResult;
import cz.vut.fit.domainradar.serialization.TagRegistry;
import cz.vut.fit.domainradar.flink.models.KafkaDomainAggregate;
import cz.vut.fit.domainradar.flink.models.KafkaDomainEntry;
import cz.vut.fit.domainradar.flink.models.KafkaIPEntry;
import cz.vut.fit.domainradar.flink.models.KafkaMergedResult;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * A Flink sink that stores collection results (meta)data into a PostgreSQL database in a structured way.
 */
public class PostgresCollectorResultSink implements Sink<KafkaMergedResult> {

    private final String dbUrl;
    private final String dbUser;
    private final String dbPassword;
    private final boolean storeRawData;

    public PostgresCollectorResultSink(String dbUrl, String dbUser, String dbPassword, boolean storeRawData) {
        this.dbUrl = Objects.requireNonNull(dbUrl, "dbUrl");
        this.dbUser = dbUser;
        this.dbPassword = dbPassword;
        this.storeRawData = storeRawData;
    }

    @Deprecated
    public SinkWriter<KafkaMergedResult> createWriter(InitContext context) throws IOException {
        return null;
    }

    @Override
    public SinkWriter<KafkaMergedResult> createWriter(WriterInitContext context) throws IOException {
        try {
            return new PgWriter();
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @SuppressWarnings("SqlNoDataSourceInspection")
    private class PgWriter extends PgWriterBase<KafkaMergedResult> {
        private static final Logger LOG = LoggerFactory.getLogger(PostgresCollectorResultSink.class);

        private final PreparedStatement selectIp;
        private final PreparedStatement insertIp;
        private final PreparedStatement insertResult;
        private final PreparedStatement updateIpGeoData;
        private final PreparedStatement updateIpNerdData;

        private final Map<String, CollectorInfo> collectorCache = new HashMap<>();
        private final TypeReference<CommonIPResult<GeoIPData>> geoRef = new TypeReference<>() {
        };
        private final TypeReference<CommonIPResult<NERDData>> nerdRef = new TypeReference<>() {
        };

        PgWriter() throws SQLException {
            super(dbUrl, dbUser, dbPassword, "flink-collector-result-sink", LOG);

            selectIp = connection.prepareStatement(
                    "SELECT id FROM IP WHERE domain_id = ? AND ip = ?::inet");
            insertIp = connection.prepareStatement(
                    "INSERT INTO IP(domain_id, ip) VALUES (?, ?::inet) ON CONFLICT(domain_id, ip) DO NOTHING RETURNING id");
            insertResult = connection.prepareStatement(
                    "INSERT INTO Collection_Result(domain_id, ip_id, source_id, status_code, error, timestamp, raw_data)" +
                            " VALUES (?, ?, ?, ?, ?, ?, ?::jsonb)" +
                            " ON CONFLICT ON CONSTRAINT collection_result_unique DO UPDATE" +
                            " SET status_code=EXCLUDED.status_code, error=EXCLUDED.error, raw_data=EXCLUDED.raw_data");
            updateIpGeoData = connection.prepareStatement(
                    "UPDATE IP SET geo_country_code=?, geo_region=?, geo_region_code=?, geo_city=?," +
                            " geo_postal_code=?, geo_latitude=?, geo_longitude=?, geo_timezone=?," +
                            " asn=?, as_org=?, network_address=?, network_prefix_length=?," +
                            " geo_asn_update_timestamp=? WHERE id=?" +
                            " AND (geo_asn_update_timestamp IS NULL OR geo_asn_update_timestamp <= ?)");
            updateIpNerdData = connection.prepareStatement(
                    "UPDATE IP SET nerd_reputation=?, nerd_update_timestamp=? WHERE id=?" +
                            " AND (nerd_update_timestamp IS NULL OR nerd_update_timestamp <= ?)");
        }

        @Override
        protected void processInput(@NotNull KafkaMergedResult merged) throws Exception {
            var domainName = merged.getDomainName();
            LOG.trace("[{}] Processing entry", domainName);

            long ts = earliestTimestamp(merged);

            // Get or create the domain row (its ID)
            long domainId = getOrCreateDomain(domainName, ts);
            // Get or create all the IP rows (their IDs)
            Map<String, Long> ipIds = new HashMap<>();
            if (merged.getIPData() != null) {
                for (var ipEntry : merged.getIPData().entrySet()) {
                    var ipId = getOrCreateIp(domainId, ipEntry.getKey(), domainName);
                    ipIds.put(ipEntry.getKey(), ipId);
                }
            }

            // Domain based results
            var domainData = merged.getDomainData();
            handleDomainEntry(domainId, domainData.getZoneData(), domainName);
            handleDomainEntry(domainId, domainData.getDNSData(), domainName);
            handleDomainEntry(domainId, domainData.getRDAPData(), domainName);
            handleDomainEntry(domainId, domainData.getTLSData(), domainName);

            // IP based results
            if (merged.getIPData() != null) {
                for (var ipEntry : merged.getIPData().entrySet()) {
                    String ip = ipEntry.getKey();
                    var ipId = ipIds.get(ip);
                    for (var collectorEntry : ipEntry.getValue().entrySet()) {
                        handleIpEntry(domainId, ipId, collectorEntry.getKey(), collectorEntry.getValue(),
                                domainName, ip);
                    }
                }
            }
        }

        private long getOrCreateIp(long domainId, String ip, String domainName) throws SQLException {
            LOG.trace("[{}][{}] Getting IP", domainName, ip);
            selectIp.setLong(1, domainId);
            selectIp.setString(2, ip);
            try (var rs = selectIp.executeQuery()) {
                if (rs.next()) {
                    return rs.getLong(1);
                }
            }
            LOG.trace("[{}][{}] IP not found, doing upsert", domainName, ip);
            insertIp.setLong(1, domainId);
            insertIp.setString(2, ip);
            try (var rs = insertIp.executeQuery()) {
                if (rs.next()) {
                    return rs.getLong(1);
                }
            }
            LOG.trace("[{}][{}] Didn't get IP from upsert, trying get again", domainName, ip);
            selectIp.setLong(1, domainId);
            selectIp.setString(2, ip);
            try (var rs = selectIp.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }

        private void handleDomainEntry(long domainId, KafkaDomainEntry entry, String domainName) throws Exception {
            if (entry == null) {
                return;
            }
            LOG.trace("[{}] Processing entry (target: {})", domainName, entry.getTopic());
            String collector = Topics.TOPICS_TO_COLLECTOR_ID.get(entry.getTopic());
            if (collector == null) {
                LOG.warn("[{}] Unknown collector for topic {}", domainName, entry.getTopic());
                insertDomainError(domainId,
                        "Unknown collector: " + entry.getTopic(), null);
                return;
            }
            CollectorInfo ci = getCollectorInfo(collector, domainId, entry.getTimestamp());
            if (ci == null) {
                LOG.warn("[{}] No collector ID found in the database for collector {}", domainName, collector);
                return;
            }
            LOG.trace("[{}] Performing collection result insert for collector {} ({})", domainName, collector, ci.id);
            insertCollectionResult(domainId, null, ci.id, entry.getStatusCode(), entry.getError(),
                    entry.getTimestamp(), entry.getValue());
        }

        private void handleIpEntry(long domainId, long ipId, byte collectorTag, KafkaIPEntry entry,
                                   String domainName, String ip) throws Exception {
            LOG.trace("[{}][{}] Processing IP entry (collector tag: {})", domainName, ipId, collectorTag);
            String collector = TagRegistry.COLLECTOR_NAMES.get((int) collectorTag);
            if (collector == null) {
                LOG.warn("[{}][{}] Unknown IP collector tag {}", domainName, ip, collectorTag);
                insertDomainError(domainId,
                        "Unknown collector tag: " + collectorTag, null);
                return;
            }
            CollectorInfo ci = getCollectorInfo(collector, domainId, entry.getTimestamp());
            if (ci == null) {
                LOG.warn("[{}][{}] No collector ID found in the database for collector {}", domainName, ip, collector);
                return;
            }

            LOG.trace("[{}][{}] Performing collection result insert for collector {} ({})", domainName, ip, collector,
                    ci.id);
            insertCollectionResult(domainId, ipId, ci.id, entry.getStatusCode(), entry.getError(),
                    entry.getTimestamp(), entry.getValue());

            if (entry.getStatusCode() == 0) {
                var result = parseIpResult(domainId, entry.getTimestamp(), collector, entry.getValue());
                if (result == null) return;
                var data = result.data();
                if (data == null) return;
                LOG.trace("[{}][{}] Updating IP metadata based on {}", domainName, ip, collector);

                if ("geo-asn".equals(collector)) {
                    updateGeoIpData(ipId, (GeoIPData) data, entry.getTimestamp());
                } else if ("nerd".equals(collector)) {
                    updateNerdData(ipId, (NERDData) data, entry.getTimestamp());
                }
            }
        }

        private @Nullable CommonIPResult<?> parseIpResult(long domainId, long ts, String collector, byte[] data)
                throws SQLException {
            try {
                if ("geo-asn".equals(collector)) {
                    return mapper.readValue(data, geoRef);
                } else if ("nerd".equals(collector)) {
                    return mapper.readValue(data, nerdRef);
                } else {
                    return null;
                }
            } catch (Exception e) {
                insertDomainError(domainId, "Cannot deserialize JSON.", e);
                return null;
            }
        }

        /**
         * Gets the collector ID and "is IP collector" flag from the cache or the database.
         *
         * @param name     The collector name.
         * @param domainId The domain ID (for error reporting).
         * @param ts       The timestamp of the entry (for error reporting).
         * @return The collector information, or null if the collector is unknown.
         * @throws SQLException If an SQL error occurs.
         */
        private CollectorInfo getCollectorInfo(String name, long domainId, long ts) throws SQLException {
            CollectorInfo ci = collectorCache.get(name);
            if (ci != null) return ci;
            try (var ps = connection.prepareStatement("SELECT id, is_ip_collector FROM Collector WHERE collector = ?")) {
                ps.setString(1, name);
                try (var rs = ps.executeQuery()) {
                    if (rs.next()) {
                        ci = new CollectorInfo(rs.getShort(1), rs.getBoolean(2));
                        collectorCache.put(name, ci);
                        return ci;
                    }
                }
            }
            insertDomainError(domainId, "Unknown collector: " + name, null);
            return null;
        }

        private boolean isJsonbValid(byte[] rawData) {
            JsonNode tree;
            try {
                tree = mapper.readTree(rawData);
                return validateJsonNode(tree);
            } catch (IOException e) {
                return false;
            }
        }

        private boolean validateJsonNode(JsonNode node) {
            final var nullChar = "\u0000";

            if (node.isObject()) {
                var fields = node.fields();
                while (fields.hasNext()) {
                    var field = fields.next();
                    if (!validateJsonNode(field.getValue())) {
                        return false;
                    }
                }
                return true;
            } else if (node.isArray()) {
                for (JsonNode element : node) {
                    if (!validateJsonNode(element)) {
                        return false;
                    }
                }
                return true;
            } else if (node.isTextual()) {
                var text = node.textValue();
                // Make a CharSequence with only the NUL character
                return !text.contains(nullChar);
            } else {
                return true;
            }
        }


        /**
         * Inserts a collection result into the database.
         *
         * @param domainId    The ID of the domain.
         * @param ipId        The ID of the IP, or null if not applicable.
         * @param collectorId The ID of the collector.
         * @param statusCode  The status code of the collection result.
         * @param error       The error message, or null if there was no error.
         * @param ts          The timestamp of the collection result.
         * @param rawData     The raw data of the collection result, or null if not applicable.
         * @throws SQLException If an SQL error occurs.
         */
        private void insertCollectionResult(long domainId, Long ipId, short collectorId, int statusCode,
                                            String error, long ts, byte[] rawData) throws SQLException {
            insertResult.setLong(1, domainId);
            if (ipId == null) {
                insertResult.setNull(2, Types.BIGINT);
            } else {
                insertResult.setLong(2, ipId);
            }
            insertResult.setShort(3, collectorId);
            insertResult.setInt(4, statusCode);
            if (error == null) {
                insertResult.setNull(5, Types.VARCHAR);
            } else {
                insertResult.setString(5, error);
            }
            insertResult.setTimestamp(6, new Timestamp(ts));

            if (storeRawData && rawData != null) {
                if (isJsonbValid(rawData)) {
                    insertResult.setString(7, new String(rawData));
                    insertResult.executeUpdate();
                    return;
                } else {
                    insertDomainError(domainId, "Cannot deserialize JSON.", null);
                }
            }

            insertResult.setNull(7, Types.VARCHAR);
            insertResult.executeUpdate();
        }

        /**
         * Updates the GEO-ASN (GeoIP) data stored within the row for a given IP.
         *
         * @param ipId The ID of the IP.
         * @param data The GEO-ASN (GeoIP) data to update.
         * @param ts   The new "GEO-ASN last updated" timestamp.
         * @throws SQLException If an SQL error occurs.
         */
        private void updateGeoIpData(long ipId, GeoIPData data, long ts) throws SQLException {
            var ps = updateIpGeoData;
            ps.setString(1, data.countryCode());
            ps.setString(2, data.region());
            ps.setString(3, data.regionCode());
            ps.setString(4, data.city());
            ps.setString(5, data.postalCode());
            ps.setObject(6, data.latitude(), Types.DOUBLE);
            ps.setObject(7, data.longitude(), Types.DOUBLE);
            ps.setString(8, data.timezone());
            ps.setObject(9, data.asn(), Types.BIGINT);
            ps.setString(10, data.asnOrg());
            ps.setString(11, data.networkAddress());
            ps.setObject(12, data.prefixLength(), Types.INTEGER);
            Timestamp tsObj = new Timestamp(ts);
            ps.setTimestamp(13, tsObj);
            ps.setLong(14, ipId);
            ps.setTimestamp(15, tsObj);
            ps.executeUpdate();
        }

        /**
         * Updates the NERD data stored within the row for a given IP.
         *
         * @param ipId The ID of the IP.
         * @param data The NERD data to update.
         * @param ts   The new "NERD last updated" timestamp.
         * @throws SQLException If an SQL error occurs.
         */
        private void updateNerdData(long ipId, NERDData data, long ts) throws SQLException {
            var ps = updateIpNerdData;
            ps.setDouble(1, data.reputation());
            Timestamp tsObj = new Timestamp(ts);
            ps.setTimestamp(2, tsObj);
            ps.setLong(3, ipId);
            ps.setTimestamp(4, tsObj);
            ps.executeUpdate();
        }

        /**
         * Returns the earliest collection timestamp from the merged result,
         * or the current time if no data is available.
         *
         * @param merged The merged result.
         * @return The earliest timestamp.
         */
        private long earliestTimestamp(KafkaMergedResult merged) {
            long ts = Long.MAX_VALUE;
            KafkaDomainAggregate d = merged.getDomainData();
            if (d.getZoneData() != null) ts = d.getZoneData().getTimestamp();
            if (d.getDNSData() != null) ts = Math.min(ts, d.getDNSData().getTimestamp());
            if (d.getRDAPData() != null) ts = Math.min(ts, d.getRDAPData().getTimestamp());
            if (d.getTLSData() != null) ts = Math.min(ts, d.getTLSData().getTimestamp());
            if (merged.getIPData() != null) {
                for (var ip : merged.getIPData().values()) {
                    for (KafkaIPEntry e : ip.values()) {
                        ts = Math.min(ts, e.getTimestamp());
                    }
                }
            }
            if (ts == Long.MAX_VALUE) ts = System.currentTimeMillis();
            return ts;
        }
    }

    private record CollectorInfo(short id, boolean isIpCollector) {
    }
}
