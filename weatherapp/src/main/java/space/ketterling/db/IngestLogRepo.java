package space.ketterling.db;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;

/**
 * Database access for ingest run logs and events.
 */
public class IngestLogRepo {
    private final HikariDataSource ds;
    private final ObjectMapper om;
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(IngestLogRepo.class);

    /**
     * Creates a repo backed by the provided datasource and JSON mapper.
     */
    public IngestLogRepo(HikariDataSource ds, ObjectMapper om) {
        this.ds = ds;
        this.om = om;
    }

    /**
     * Starts a new ingest run and returns its unique ID.
     */
    public UUID startRun(String jobName) throws Exception {
        UUID runId = UUID.randomUUID();
        if (ds.isClosed()) {
            log.warn("startRun skipped (datasource closed): {}", jobName);
            return runId;
        }
        try (Connection c = ds.getConnection();
                PreparedStatement ps = c.prepareStatement(
                        "INSERT INTO ingest_run (run_id, job_name, started_at, status) VALUES (?, ?, now(), 'RUNNING')")) {
            ps.setObject(1, runId);
            ps.setString(2, jobName);
            ps.executeUpdate();
        } catch (SQLException e) {
            if (ds.isClosed() || String.valueOf(e.getMessage()).toLowerCase().contains("closed")) {
                log.warn("startRun skipped (datasource closed): {}", jobName);
                return runId;
            }
            throw e;
        }
        log.debug("startRun: {} -> {}", jobName, runId);
        return runId;
    }

    /**
     * Marks an ingest run as success or failure with notes.
     */
    public void finishRun(UUID runId, boolean success, String notes) throws Exception {
        if (ds.isClosed()) {
            log.warn("finishRun skipped (datasource closed): {}", runId);
            return;
        }
        try (Connection c = ds.getConnection();
                PreparedStatement ps = c.prepareStatement(
                        "UPDATE ingest_run SET finished_at=now(), status=?, notes=? WHERE run_id=?")) {
            ps.setString(1, success ? "SUCCESS" : "FAILED");
            ps.setString(2, notes);
            ps.setObject(3, runId);
            ps.executeUpdate();
        } catch (SQLException e) {
            if (ds.isClosed() || String.valueOf(e.getMessage()).toLowerCase().contains("closed")) {
                log.warn("finishRun skipped (datasource closed): {}", runId);
                return;
            }
            throw e;
        }
        log.debug("finishRun: {} success={} notes={}", runId, success, notes);
    }

    /**
     * Logs a single external request event for an ingest run.
     */
    public void logEvent(UUID runId, String source, String endpoint, Integer httpStatus, Integer responseMs,
            String error, Map<String, String> headersFlat) throws Exception {

        if (ds.isClosed()) {
            log.warn("logEvent skipped (datasource closed): {} {}", source, endpoint);
            return;
        }

        String sql = "INSERT INTO ingest_event (run_id, source, endpoint, http_status, response_ms, error, response_headers, created_at) "
                +
                "VALUES (?, ?, ?, ?, ?, ?, ?::jsonb, now())";

        String headersJson = (headersFlat == null) ? "{}" : om.writeValueAsString(headersFlat);

        try (Connection c = ds.getConnection();
                PreparedStatement ps = c.prepareStatement(sql)) {

            ps.setObject(1, runId);
            ps.setString(2, source);
            ps.setString(3, endpoint);

            if (httpStatus == null)
                ps.setNull(4, java.sql.Types.INTEGER);
            else
                ps.setInt(4, httpStatus);

            if (responseMs == null)
                ps.setNull(5, java.sql.Types.INTEGER);
            else
                ps.setInt(5, responseMs);

            ps.setString(6, error);
            ps.setString(7, headersJson);

            ps.executeUpdate();
        } catch (SQLException e) {
            if (ds.isClosed() || String.valueOf(e.getMessage()).toLowerCase().contains("closed")) {
                log.warn("logEvent skipped (datasource closed): {} {}", source, endpoint);
                return;
            }
            throw e;
        }
        log.debug("logEvent: run={} source={} endpoint={} status={} ms={} error={}", runId, source, endpoint,
                httpStatus, responseMs, error);
    }
}
