package cz.vut.fit.domainradar.db;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import cz.vut.fit.domainradar.*;
import cz.vut.fit.domainradar.models.ip.GeoIPData;
import cz.vut.fit.domainradar.models.ip.NERDData;
import cz.vut.fit.domainradar.models.results.CommonIPResult;
import cz.vut.fit.domainradar.serialization.TagRegistry;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * A Flink sink that stores {@link KafkaMergedResult} objects into a PostgreSQL database.
 * It performs the same operations as the former {@code process_collection_results()} procedure.
 */
public class PostgresCollectorResultSink implements Sink<KafkaMergedResult> {

    private final String dbUrl;
    private final String dbUser;
    private final String dbPassword;

    public PostgresCollectorResultSink(String dbUrl, String dbUser, String dbPassword) {
        this.dbUrl = Objects.requireNonNull(dbUrl, "dbUrl");
        this.dbUser = dbUser;
        this.dbPassword = dbPassword;
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

    private class PgWriter implements SinkWriter<KafkaMergedResult> {
        private final Logger LOG = LoggerFactory.getLogger(PostgresCollectorResultSink.class);

        private final Connection connection;
        private final PreparedStatement selectDomain;
        private final PreparedStatement insertDomain;
        private final PreparedStatement selectIp;
        private final PreparedStatement insertIp;
        private final PreparedStatement insertResult;
        private final PreparedStatement insertDomainError;
        private final PreparedStatement updateIpGeoData;
        private final PreparedStatement updateIpNerdData;

        private final ObjectMapper mapper;
        private final Map<String, CollectorInfo> collectorCache = new HashMap<>();
        private final TypeReference<CommonIPResult<GeoIPData>> geoRef = new TypeReference<>() {
        };
        private final TypeReference<CommonIPResult<NERDData>> nerdRef = new TypeReference<>() {
        };

        PgWriter() throws SQLException {
            LOG.debug("Opening connection");
            connection = DriverManager.getConnection(dbUrl, dbUser, dbPassword);
            connection.setAutoCommit(false);

            selectDomain = connection.prepareStatement("SELECT id FROM Domain WHERE domain_name = ?");
            insertDomain = connection.prepareStatement(
                    "INSERT INTO Domain(domain_name, last_update) VALUES (?, ?)" +
                            " ON CONFLICT (domain_name) DO UPDATE SET last_update=EXCLUDED.last_update" +
                            " RETURNING id");
            selectIp = connection.prepareStatement(
                    "SELECT id FROM IP WHERE domain_id = ? AND ip = ?::inet");
            insertIp = connection.prepareStatement(
                    "INSERT INTO IP(domain_id, ip) VALUES (?, ?::inet) ON CONFLICT(domain_id, ip) DO NOTHING RETURNING id");
            insertResult = connection.prepareStatement(
                    "INSERT INTO Collection_Result(domain_id, ip_id, source_id, status_code, error, timestamp, raw_data)" +
                            " VALUES (?, ?, ?, ?, ?, ?, ?::jsonb)" +
                            " ON CONFLICT ON CONSTRAINT collection_result_unique DO UPDATE" +
                            " SET status_code=EXCLUDED.status_code, error=EXCLUDED.error, raw_data=EXCLUDED.raw_data");
            insertDomainError = connection.prepareStatement(
                    "INSERT INTO Domain_Errors(domain_id, timestamp, source, error, sql_error_code, sql_error_message)" +
                            " VALUES (?, ?, ?, ?, ?, ?)");
            updateIpGeoData = connection.prepareStatement(
                    "UPDATE IP SET geo_country_code=?, geo_region=?, geo_region_code=?, geo_city=?," +
                            " geo_postal_code=?, geo_latitude=?, geo_longitude=?, geo_timezone=?," +
                            " asn=?, as_org=?, network_address=?, network_prefix_length=?," +
                            " geo_asn_update_timestamp=? WHERE id=?" +
                            " AND (geo_asn_update_timestamp IS NULL OR geo_asn_update_timestamp <= ?)");
            updateIpNerdData = connection.prepareStatement(
                    "UPDATE IP SET nerd_reputation=?, nerd_update_timestamp=? WHERE id=?" +
                            " AND (nerd_update_timestamp IS NULL OR nerd_update_timestamp <= ?)");

            mapper = Common.makeMapper().build();
        }

        @Override
        public void write(KafkaMergedResult value, Context context) throws IOException {
            try {
                processResult(value);
                LOG.debug("Issued all commands, commiting");
                connection.commit();
            } catch (Exception e) {
                try {
                    LOG.warn("Rolling back due to an error");
                    connection.rollback();
                } catch (SQLException ex) {
                    LOG.error("Rollback failed", ex);
                }
                throw new IOException("Failed to store merged result", e);
            }
        }

        @Override
        public void flush(boolean endOfInput) throws IOException {
            try {
                LOG.trace("Flush – commiting");
                connection.commit();
            } catch (SQLException e) {
                throw new IOException(e);
            }
        }

        @Override
        public void close() throws Exception {
            LOG.debug("Closing connection");
            connection.close();
        }

        private void processResult(KafkaMergedResult merged) throws Exception {
            var domainName = merged.getDomainName();
            LOG.trace("[{}] Processing entry", domainName);

            long ts = earliestTimestamp(merged);
            long domainId = getOrCreateDomain(domainName, ts);

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

        private long getOrCreateDomain(String domainName, long timestamp) throws SQLException {
            LOG.trace("[{}] Getting domain ID", domainName);
            selectDomain.setString(1, domainName);
            try (var rs = selectDomain.executeQuery()) {
                if (rs.next()) {
                    return rs.getLong(1);
                }
            }
            LOG.trace("[{}] Domain not found, doing upsert", domainName);
            insertDomain.setString(1, domainName);
            insertDomain.setTimestamp(2, new Timestamp(timestamp));
            try (var rs = insertDomain.executeQuery()) {
                if (rs.next()) {
                    return rs.getLong(1);
                }
            }
            LOG.trace("[{}] Didn't get ID from upsert, trying get again", domainName);
            selectDomain.setString(1, domainName);
            try (var rs = selectDomain.executeQuery()) {
                rs.next();
                return rs.getLong(1);
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
            } catch (SQLException e) {
                if (e.getSQLState().equals("22P02")) {
                    // Invalid IP (cast error) - insert with a placeholder IP (unless we're already doing that)
                    if (!ip.equals("0.0.0.0")) {
                        return this.getOrCreateIp(domainId, "0.0.0.0", domainName);
                    } else {
                        // Shouldn't happen
                        throw e;
                    }
                } else {
                    // The error is not cast-related, propagate
                    throw e;
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
                insertDomainError(domainId, entry.getTimestamp(),
                        "Unknown collector: " + entry.getTopic(), null);
                return;
            }
            CollectorInfo ci = getCollectorInfo(domainId, entry.getTimestamp(), collector);
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
                insertDomainError(domainId, entry.getTimestamp(),
                        "Unknown collector tag: " + collectorTag, null);
                return;
            }
            CollectorInfo ci = getCollectorInfo(domainId, entry.getTimestamp(), collector);
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
                insertDomainError(domainId, ts, "Cannot parse JSON.", e);
                return null;
            }
        }

        private void insertDomainError(long domainId, long ts, String message, Exception e) throws SQLException {
            insertDomainError.setLong(1, domainId);
            insertDomainError.setTimestamp(2, new Timestamp(ts));
            insertDomainError.setString(3, "flink-collection-results-db-sink");
            insertDomainError.setString(4, message);
            if (e instanceof SQLException se) {
                insertDomainError.setString(5, se.getSQLState());
                insertDomainError.setString(6, se.getMessage());
            } else {
                insertDomainError.setNull(5, Types.VARCHAR);
                insertDomainError.setString(6, e == null ? null : e.getMessage());
            }
            insertDomainError.executeUpdate();
        }

        private CollectorInfo getCollectorInfo(long domainId, long ts, String name) throws SQLException {
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
            insertDomainError(domainId, ts, "Unknown collector: " + name, null);
            return null;
        }

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
            insertResult.setString(7, new String(rawData));
            insertResult.executeUpdate();
        }

        private void updateGeoIpData(long ipId, GeoIPData data, long ts) throws SQLException {
            var ps = updateIpGeoData;
            ps.setString(1, data.countryCode());
            ps.setString(2, data.region());
            ps.setString(3, data.regionCode());
            ps.setString(4, data.city());
            ps.setString(5, data.postalCode());
            setDoubleOrNull(ps, 6, data.latitude());
            setDoubleOrNull(ps, 7, data.longitude());
            ps.setString(8, data.timezone());
            setLongOrNull(ps, 9, data.asn());
            ps.setString(10, data.asnOrg());
            ps.setString(11, data.networkAddress());
            setIntOrNull(ps, 12, data.prefixLength() == null ? null : data.prefixLength().intValue());
            Timestamp tsObj = new Timestamp(ts);
            ps.setTimestamp(13, tsObj);
            ps.setLong(14, ipId);
            ps.setTimestamp(15, tsObj);
            ps.executeUpdate();
        }

        private void updateNerdData(long ipId, NERDData data, long ts) throws SQLException {
            var ps = updateIpNerdData;
            setDoubleOrNull(ps, 1, data.reputation());
            Timestamp tsObj = new Timestamp(ts);
            ps.setTimestamp(2, tsObj);
            ps.setLong(3, ipId);
            ps.setTimestamp(4, tsObj);
            ps.executeUpdate();
        }

        private void setDoubleOrNull(PreparedStatement ps, int idx, Double n) throws SQLException {
            if (n == null) {
                ps.setNull(idx, Types.DOUBLE);
            } else {
                ps.setDouble(idx, n);
            }
        }

        private void setLongOrNull(PreparedStatement ps, int idx, Long n) throws SQLException {
            if (n == null) {
                ps.setNull(idx, Types.BIGINT);
            } else {
                ps.setLong(idx, n);
            }
        }

        private void setIntOrNull(PreparedStatement ps, int idx, Integer n) throws SQLException {
            if (n == null) {
                ps.setNull(idx, Types.INTEGER);
            } else {
                ps.setInt(idx, n);
            }
        }

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
