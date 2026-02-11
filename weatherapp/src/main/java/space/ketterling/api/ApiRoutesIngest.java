package space.ketterling.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.zaxxer.hikari.HikariDataSource;
import io.javalin.Javalin;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * Routes that show ingestion runs and events (for debugging data pipelines).
 */
final class ApiRoutesIngest {
    /**
     * Utility class; do not instantiate.
     */
    private ApiRoutesIngest() {
    }

    /**
     * Registers ingest log endpoints.
     */
    static void register(ApiServer api) {
        Javalin app = api.app();
        ObjectMapper om = api.om();
        HikariDataSource ds = api.ds();

        app.get("/api/ingest/runs", ctx -> {
            int limit = ApiServer.parseInt(ctx.queryParam("limit"), 50, 1, 200);

            String sql = """
                        SELECT run_id, job_name, started_at, finished_at, status, notes
                        FROM ingest_run
                        ORDER BY started_at DESC
                        LIMIT ?
                    """;

            ArrayNode arr = om.createArrayNode();
            try (Connection c = ds.getConnection();
                    PreparedStatement ps = c.prepareStatement(sql)) {
                ps.setInt(1, limit);

                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        ObjectNode row = om.createObjectNode();
                        row.put("run_id", rs.getString("run_id"));
                        row.put("job_name", rs.getString("job_name"));
                        row.put("started_at", String.valueOf(rs.getObject("started_at")));
                        row.put("finished_at", String.valueOf(rs.getObject("finished_at")));
                        row.put("status", rs.getString("status"));
                        row.put("notes", rs.getString("notes"));
                        arr.add(row);
                    }
                }
            }

            ctx.json(arr);
        });

        app.get("/api/ingest/events", ctx -> {
            int limit = ApiServer.parseInt(ctx.queryParam("limit"), 200, 1, 1000);
            String runId = ctx.queryParam("runId");

            String sql = """
                        SELECT event_id, run_id, source, endpoint, http_status, response_ms, error, created_at
                        FROM ingest_event
                        WHERE (? IS NULL OR run_id::text = ?)
                        ORDER BY created_at DESC
                        LIMIT ?
                    """;

            ArrayNode arr = om.createArrayNode();
            try (Connection c = ds.getConnection();
                    PreparedStatement ps = c.prepareStatement(sql)) {

                ps.setString(1, runId);
                ps.setString(2, runId);
                ps.setInt(3, limit);

                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        ObjectNode row = om.createObjectNode();
                        row.put("event_id", rs.getLong("event_id"));
                        row.put("run_id", String.valueOf(rs.getObject("run_id")));
                        row.put("source", rs.getString("source"));
                        row.put("endpoint", rs.getString("endpoint"));
                        api.putNullable(row, "http_status", rs.getObject("http_status"));
                        api.putNullable(row, "response_ms", rs.getObject("response_ms"));
                        row.put("error", rs.getString("error"));
                        row.put("created_at", String.valueOf(rs.getObject("created_at")));
                        arr.add(row);
                    }
                }
            }

            ctx.json(arr);
        });
    }
}
