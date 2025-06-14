package cz.vut.fit.domainradar.flink.sinks;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.vut.fit.domainradar.Common;
import cz.vut.fit.domainradar.flink.models.HasDomainName;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;

import java.io.IOException;
import java.sql.*;
import java.time.Instant;

@SuppressWarnings("SqlNoDataSourceInspection")
abstract class PgWriterBase<T extends HasDomainName> implements SinkWriter<T> {
    protected final Connection connection;
    protected final Logger logger;
    protected final ObjectMapper mapper;

    private final String entrySourceId;
    private final PreparedStatement insertDomainError;
    private final PreparedStatement selectDomain;
    private final PreparedStatement insertDomain;

    public PgWriterBase(String dbUrl, String dbUser, String dbPassword,
                        String entrySourceId, Logger logger) throws SQLException {
        this.logger = logger;
        this.entrySourceId = entrySourceId;

        mapper = Common.makeMapper().build();

        logger.debug("Opening connection");
        connection = DriverManager.getConnection(dbUrl, dbUser, dbPassword);
        connection.setAutoCommit(false);

        selectDomain = connection.prepareStatement("SELECT id FROM Domain WHERE domain_name = ?");
        insertDomain = connection.prepareStatement(
                "INSERT INTO Domain(domain_name, last_update) VALUES (?, ?)" +
                        " ON CONFLICT (domain_name) DO UPDATE SET last_update=EXCLUDED.last_update" +
                        " RETURNING id");
        insertDomainError = connection.prepareStatement(
                "INSERT INTO Domain_Errors(domain_id, timestamp, source, error, sql_error_code, sql_error_message)" +
                        " VALUES (?, ?, ?, ?, ?, ?)");
    }

    protected abstract void processInput(@NotNull T value) throws Exception;

    @Override
    public void write(T value, Context context) throws IOException {
        try {
            this.processInput(value);
            logger.debug("Issued all commands, commiting");
            connection.commit();
        } catch (Exception e) {
            try {
                logger.warn("Rolling back due to an error");
                connection.rollback();
            } catch (SQLException ex) {
                logger.error("Rollback failed", ex);
            }
            this.tryInsertDomainError(value.getDomainName(),
                    "Unhandled processing exception", e);
            throw new IOException("Failed to process an entry", e);
        }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException {
        try {
            logger.trace("Flush – commiting");
            connection.commit();
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void close() throws Exception {
        logger.debug("Closing connection");
        connection.close();
    }

    /**
     * Gets the ID of a domain, or creates it if it does not exist.
     *
     * @param domainName The name of the domain.
     * @param timestamp  The "last modified" timestamp to set for the domain.
     * @return The database ID of the domain.
     * @throws SQLException If an SQL error occurs.
     */
    protected long getOrCreateDomain(String domainName, long timestamp) throws SQLException {
        logger.trace("[{}] Getting domain ID", domainName);
        selectDomain.setString(1, domainName);
        try (var rs = selectDomain.executeQuery()) {
            if (rs.next()) {
                return rs.getLong(1);
            }
        }
        logger.trace("[{}] Domain not found, doing upsert", domainName);
        insertDomain.setString(1, domainName);
        insertDomain.setTimestamp(2, new Timestamp(timestamp));
        try (var rs = insertDomain.executeQuery()) {
            if (rs.next()) {
                return rs.getLong(1);
            }
        }
        logger.trace("[{}] Didn't get ID from upsert, trying get again", domainName);
        selectDomain.setString(1, domainName);
        try (var rs = selectDomain.executeQuery()) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private void tryInsertDomainError(String domainName, String message, Exception e) {
        try {
            logger.debug("[{}] Inserting top-level domain error", domainName);
            long domainId = getOrCreateDomain(domainName, Instant.now().toEpochMilli());
            insertDomainError(domainId, message, e);
            connection.commit();
        } catch (SQLException ex) {
            logger.error("[{}] Failed to insert top-level domain error", domainName, ex);
            try {
                connection.rollback();
            } catch (SQLException exc) {
                logger.error("Rollback failed", ex);
            }
        }
    }

    /**
     * Inserts an error entry into the Domain_Errors table.
     *
     * @param domainId The ID of the domain.
     * @param message  The error message.
     * @param e        The exception that caused the error, or null if not applicable.
     * @throws SQLException If an SQL error occurs.
     */
    protected void insertDomainError(long domainId, String message, Exception e) throws SQLException {
        insertDomainError.setLong(1, domainId);
        insertDomainError.setTimestamp(2, Timestamp.from(Instant.now()));
        insertDomainError.setString(3, this.entrySourceId);
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

}
