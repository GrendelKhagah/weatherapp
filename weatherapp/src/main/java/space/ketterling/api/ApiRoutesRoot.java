package space.ketterling.api;

import com.zaxxer.hikari.HikariDataSource;
import io.javalin.Javalin;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.OffsetDateTime;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Root and health endpoints for the API.
 */
final class ApiRoutesRoot {
    /**
     * Utility class; do not instantiate.
     */
    private ApiRoutesRoot() {
    }

    /**
     * Registers root and health endpoints.
     */
    static void register(ApiServer api) {
        Javalin app = api.app();
        HikariDataSource ds = api.ds();

        app.get("/", ctx -> ctx.json(Map.of(
                "service", "weatherapp",
                "status", "ok",
                "endpoints", new String[] {
                        "GET /health",
                        "GET /api/metrics/summary",
                        "GET /api/metrics/external",
                        "GET /api/gridpoints (GeoJSON)",
                        "GET /api/gridpoints/list",
                        "GET /api/alerts (GeoJSON)",
                        "GET /api/alerts/active",
                        "GET /api/forecast/hourly?gridId=LOX:149,46&limit=96",
                        "GET /api/forecast/hourly/point?lat=34.05&lon=-118.4",
                        "GET /api/stations/near?lat=34.05&lon=-118.4",
                        "GET /api/point/summary?lat=34.05&lon=-118.4",
                        "GET /api/tracked-points",
                        "GET /api/ingest/runs",
                        "GET /api/ingest/events?runId=<uuid>",
                        "GET /api/ml/runs",
                        "GET /api/ml/predictions/latest?gridId=LOX:149,46",
                        "GET /api/ml/weather/latest?sourceType=point&lat=34.05&lon=-118.4"
                })));

        app.get("/health", ctx -> {
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("status", "ok");
            out.put("time", OffsetDateTime.now().toString());

            try (Connection c = ds.getConnection();
                    PreparedStatement ps = c.prepareStatement("SELECT 1");
                    ResultSet rs = ps.executeQuery()) {
                out.put("db", rs.next() ? "ok" : "unknown");
            } catch (Exception e) {
                out.put("db", "fail");
                out.put("db_error", e.getMessage());
                ctx.status(503);
            }

            ctx.json(out);
        });
    }
}
