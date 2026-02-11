package space.ketterling.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.zaxxer.hikari.HikariDataSource;
import io.javalin.Javalin;
import space.ketterling.metrics.ExternalApiMetrics;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * Routes that return counts and health metrics for the UI dashboard.
 */
final class ApiRoutesMetrics {
    /**
     * Utility class; do not instantiate.
     */
    private ApiRoutesMetrics() {
    }

    /**
     * Registers metric endpoints.
     */
    static void register(ApiServer api) {
        Javalin app = api.app();
        ObjectMapper om = api.om();
        HikariDataSource ds = api.ds();

        app.get("/api/metrics/summary", ctx -> {
            String cacheKey = "metrics:summary";
            if (api.serveCached(ctx, cacheKey)) {
                return;
            }
            ObjectNode out = om.createObjectNode();

            String sql = """
                            SELECT
                                (SELECT COUNT(*) FROM geo_gridpoint) AS gridpoints,
                                (SELECT COUNT(*) FROM nws_forecast_hourly) AS hourly_rows,
                                (SELECT COUNT(*) FROM nws_alert) AS alert_rows,
                                (SELECT COUNT(*) FROM noaa_station) AS noaa_stations,
                                (SELECT COUNT(DISTINCT station_id) FROM noaa_daily_summary) AS noaa_stations_with_data,
                                (SELECT COUNT(*) FROM noaa_daily_summary) AS noaa_daily_rows,
                                (SELECT COUNT(*) FROM tracked_point) AS tracked_points,
                                (SELECT MAX(ingested_at) FROM nws_forecast_hourly) AS latest_hourly_ingest,
                                (SELECT MAX(ingested_at) FROM nws_alert) AS latest_alert_ingest
                    """;

            try (Connection c = ds.getConnection();
                    PreparedStatement ps = c.prepareStatement(sql);
                    ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    out.put("gridpoints", rs.getLong("gridpoints"));
                    out.put("hourly_rows", rs.getLong("hourly_rows"));
                    out.put("alert_rows", rs.getLong("alert_rows"));
                    out.put("noaa_stations", rs.getLong("noaa_stations"));
                    out.put("noaa_stations_with_data", rs.getLong("noaa_stations_with_data"));
                    out.put("noaa_daily_rows", rs.getLong("noaa_daily_rows"));
                    out.put("tracked_points", rs.getLong("tracked_points"));
                    out.put("latest_hourly_ingest", String.valueOf(rs.getObject("latest_hourly_ingest")));
                    out.put("latest_alert_ingest", String.valueOf(rs.getObject("latest_alert_ingest")));
                }
            }

            try {
                api.cacheAndRespond(ctx, cacheKey, out, 15, 30);
            } catch (Exception e) {
                ctx.json(out);
            }
        });

        app.get("/api/metrics/external", ctx -> {
            ObjectNode out = om.createObjectNode();
            out.put("window_minutes", ExternalApiMetrics.windowMinutes());
            ArrayNode services = out.putArray("services");

            var snapshots = ExternalApiMetrics.snapshot();
            for (var e : snapshots.entrySet()) {
                var snap = e.getValue();
                ObjectNode row = om.createObjectNode();
                row.put("service", e.getKey());
                row.put("calls_last_hour", snap.callsLastHour);
                row.put("failures_last_hour", snap.failuresLastHour);
                row.put("failure_pct", snap.failurePct);
                row.put("status", snap.status);
                services.add(row);
            }

            ctx.json(out);
        });
    }
}
