package cz.vut.fit.domainradar.flink.sinks;

import com.fasterxml.jackson.core.type.TypeReference;
import cz.vut.fit.domainradar.flink.models.KafkaIPEntry;
import cz.vut.fit.domainradar.models.ip.QRadarData;
import cz.vut.fit.domainradar.models.results.CommonIPResult;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.Objects;

/**
 * A sink that stores QRadar results into PostgreSQL.
 */
public class PostgresQRadarSink implements Sink<KafkaIPEntry> {
    private final String dbUrl;
    private final String dbUser;
    private final String dbPassword;

    public PostgresQRadarSink(String dbUrl, String dbUser, String dbPassword) {
        this.dbUrl = Objects.requireNonNull(dbUrl, "dbUrl");
        this.dbUser = dbUser;
        this.dbPassword = dbPassword;
    }

    @Deprecated
    public SinkWriter<KafkaIPEntry> createWriter(InitContext context) throws IOException {
        return null;
    }

    @Override
    public SinkWriter<KafkaIPEntry> createWriter(WriterInitContext context) throws IOException {
        try {
            return new PostgresQRadarSink.PgWriter();
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @SuppressWarnings("SqlNoDataSourceInspection")
    private class PgWriter extends PgWriterBase<KafkaIPEntry> {
        private static final Logger LOG = LoggerFactory.getLogger(PostgresQRadarSink.class);

        private final PreparedStatement upsertSource;
        private final PreparedStatement insertDefaultSource;
        private final PreparedStatement upsertOffense;
        private final PreparedStatement linkOffenseSource;
        private final TypeReference<CommonIPResult<QRadarData>> qradarRef = new TypeReference<>() {
        };

        PgWriter() throws SQLException {
            super(dbUrl, dbUser, dbPassword, "flink-qradar-sink", LOG);

            upsertSource = connection.prepareStatement(
                    "INSERT INTO QRadar_Offense_Source(id, ip, qradar_domain_id, magnitude)" +
                            " VALUES (?, ?::inet, ?, ?)" +
                            " ON CONFLICT (id) DO UPDATE" +
                            " SET ip=EXCLUDED.ip, qradar_domain_id=EXCLUDED.qradar_domain_id, magnitude=EXCLUDED.magnitude");
            insertDefaultSource = connection.prepareStatement(
                    "INSERT INTO QRadar_Offense_Source(id, ip, qradar_domain_id, magnitude)" +
                            " VALUES (?, NULL, -1, -1.0) ON CONFLICT (id) DO NOTHING");
            upsertOffense = connection.prepareStatement(
                    "INSERT INTO QRadar_Offense(id, description, event_count, flow_count, device_count," +
                            " severity, magnitude, last_updated_time, status)" +
                            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)" +
                            " ON CONFLICT (id) DO UPDATE SET description=EXCLUDED.description," +
                            " event_count=EXCLUDED.event_count, flow_count=EXCLUDED.flow_count," +
                            " device_count=EXCLUDED.device_count, severity=EXCLUDED.severity," +
                            " magnitude=EXCLUDED.magnitude, last_updated_time=EXCLUDED.last_updated_time," +
                            " status=EXCLUDED.status");
            linkOffenseSource = connection.prepareStatement(
                    "INSERT INTO QRadar_Offense_In_Source(offense_source_id, offense_id)" +
                            " VALUES (?, ?) ON CONFLICT (offense_id, offense_source_id) DO NOTHING");
        }

        @Override
        protected void processInput(@NotNull KafkaIPEntry entry) throws Exception {
            long ts = entry.getTimestamp();
            long domainId = getOrCreateDomain(entry.getDomainName(), ts);
            var data = deserializeQRadarResult(entry, domainId);
            if (data == null) {
                return; // Deserialization failed, already logged
            }

            upsertSource.setLong(1, data.sourceAddressId());
            upsertSource.setString(2, entry.getIP());
            upsertSource.setInt(3, (int) data.qradarDomainId());
            upsertSource.setDouble(4, data.magnitude());
            upsertSource.executeUpdate();

            for (var offense : data.offenses()) {
                upsertOffense.setLong(1, offense.id());
                upsertOffense.setString(2, offense.description());
                upsertOffense.setLong(3, offense.eventCount());
                upsertOffense.setLong(4, offense.flowCount());
                upsertOffense.setLong(5, offense.deviceCount());
                upsertOffense.setDouble(6, offense.severity());
                upsertOffense.setDouble(7, offense.magnitude());
                upsertOffense.setTimestamp(8, new Timestamp(offense.lastUpdatedTime()));
                upsertOffense.setString(9, offense.status());
                upsertOffense.executeUpdate();

                if (offense.sourceAddressIds() != null) {
                    for (long srcId : offense.sourceAddressIds()) {
                        insertDefaultSource.setLong(1, srcId);
                        insertDefaultSource.executeUpdate();
                        linkOffenseSource.setLong(1, srcId);
                        linkOffenseSource.setLong(2, offense.id());
                        linkOffenseSource.executeUpdate();
                    }
                }
            }
        }

        private QRadarData deserializeQRadarResult(KafkaIPEntry entry, long domainId)
                throws SQLException {
            try {
                var data = mapper.readValue(entry.getValue(), qradarRef);
                return data.data();
            } catch (Exception e) {
                LOG.warn("[{}][{}] Failed to deserialize QRadar data", entry.getDomainName(), entry.getIP(), e);
                insertDomainError(domainId, entry.getTimestamp(),
                        "Cannot parse QRadar result JSON.", e);
                return null;
            }
        }
    }
}