package space.ketterling.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.zaxxer.hikari.HikariDataSource;
import io.javalin.Javalin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * Routes that expose machine-learning predictions and forecasts.
 */
final class ApiRoutesMl {
    private static final Logger log = LoggerFactory.getLogger(ApiRoutesMl.class);

    /**
     * Utility class; do not instantiate.
     */
    private ApiRoutesMl() {
    }

    /**
     * Registers ML endpoints used by the UI.
     */
    static void register(ApiServer api) {
        Javalin app = api.app();
        ObjectMapper om = api.om();
        HikariDataSource ds = api.ds();

        app.get("/api/ml/runs", ctx -> {
            int limit = ApiServer.parseInt(ctx.queryParam("limit"), 25, 1, 200);

            String sql = """
                        SELECT run_id, model_name, feature_version, train_start, train_end, dataset_version, split_strategy, created_at
                        FROM ml_model_run
                        ORDER BY created_at DESC
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
                        row.put("model_name", rs.getString("model_name"));
                        row.put("feature_version", rs.getString("feature_version"));
                        row.put("train_start", String.valueOf(rs.getObject("train_start")));
                        row.put("train_end", String.valueOf(rs.getObject("train_end")));
                        row.put("dataset_version", rs.getString("dataset_version"));
                        row.put("split_strategy", rs.getString("split_strategy"));
                        row.put("created_at", String.valueOf(rs.getObject("created_at")));
                        arr.add(row);
                    }
                }
            } catch (Exception e) {
                log.warn("ML runs query failed (ok if not implemented yet): {}", e.getMessage());
            }

            ctx.json(arr);
        });

        app.get("/api/ml/predictions/latest", ctx -> {
            String gridId = ApiServer.firstNonBlank(ctx.queryParam("gridId"), ctx.queryParam("grid_id"));
            if (gridId == null || gridId.isBlank()) {
                ctx.status(400).json(om.createObjectNode().put("error", "gridId (or grid_id) is required"));
                return;
            }

            int limit = ApiServer.parseInt(ctx.queryParam("limit"), 48, 1, 240);

            String sql = """
                        SELECT p.run_id, p.grid_id, p.valid_time, p.horizon_hours, p.risk_score, p.risk_class, p.created_at
                        FROM ml_prediction p
                        JOIN (
                            SELECT run_id
                            FROM ml_model_run
                            ORDER BY created_at DESC
                            LIMIT 1
                        ) r ON p.run_id = r.run_id
                        WHERE p.grid_id = ?
                        ORDER BY p.valid_time ASC
                        LIMIT ?
                    """;

            ArrayNode arr = om.createArrayNode();
            try (Connection c = ds.getConnection();
                    PreparedStatement ps = c.prepareStatement(sql)) {
                ps.setString(1, gridId);
                ps.setInt(2, limit);

                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        ObjectNode row = om.createObjectNode();
                        row.put("run_id", rs.getString("run_id"));
                        row.put("grid_id", rs.getString("grid_id"));
                        row.put("valid_time", String.valueOf(rs.getObject("valid_time")));
                        row.put("horizon_hours", rs.getInt("horizon_hours"));
                        api.putNullable(row, "risk_score", rs.getObject("risk_score"));
                        row.put("risk_class", rs.getString("risk_class"));
                        row.put("created_at", String.valueOf(rs.getObject("created_at")));
                        arr.add(row);
                    }
                }
            } catch (Exception e) {
                log.warn("ML predictions query failed (ok if not implemented yet): {}", e.getMessage());
            }

            ctx.json(arr);
        });

        app.get("/api/ml/weather/latest", ctx -> {
            String sourceType = ctx.queryParam("sourceType");
            String sourceId = ctx.queryParam("sourceId");
            Double lat = null;
            Double lon = null;
            try {
                if (ctx.queryParam("lat") != null)
                    lat = Double.parseDouble(ctx.queryParam("lat"));
                if (ctx.queryParam("lon") != null)
                    lon = Double.parseDouble(ctx.queryParam("lon"));
            } catch (Exception ignored) {
            }

            if (sourceType == null || sourceType.isBlank()) {
                ctx.status(400).json(om.createObjectNode().put("error", "sourceType is required"));
                return;
            }

            String effectiveSourceType = sourceType;

            if (sourceId != null && !sourceId.isBlank()) {
                lat = null;
                lon = null;
            }

            if ((effectiveSourceType.equals("gridpoint") || effectiveSourceType.equals("station")
                    || effectiveSourceType.equals("tracked"))
                    && lat != null && lon != null && !ApiServer.isLatLonValid(lat, lon)) {
                ctx.status(400).json(om.createObjectNode().put("error", "invalid lat/lon"));
                return;
            }

            String sql = """
                            SELECT source_type, source_id, lat, lon, as_of_date, horizon_hours,
                                         tmean_c, prcp_mm, tmin_c, tmax_c, delta_c, model_name, model_detail, confidence, created_at
                            FROM ml_weather_prediction
                            WHERE source_type = ?
                                AND (? IS NULL OR source_id = ?)
                                AND (? IS NULL OR lat BETWEEN ? AND ?)
                                AND (? IS NULL OR lon BETWEEN ? AND ?)
                            ORDER BY created_at DESC
                            LIMIT 1
                    """;
            ObjectNode out = om.createObjectNode();
            try (Connection c = ds.getConnection()) {
                String[] sourceTypes = "point".equals(sourceType)
                        ? new String[] { "point", "gridpoint" }
                        : new String[] { effectiveSourceType };

                for (String st : sourceTypes) {
                    try (PreparedStatement ps = c.prepareStatement(sql)) {
                        ps.setString(1, st);
                        ps.setString(2, sourceId);
                        ps.setString(3, sourceId);

                        if (lat != null) {
                            ps.setDouble(4, lat);
                            ps.setDouble(5, lat - 0.01);
                            ps.setDouble(6, lat + 0.01);
                        } else {
                            ps.setNull(4, java.sql.Types.DOUBLE);
                            ps.setNull(5, java.sql.Types.DOUBLE);
                            ps.setNull(6, java.sql.Types.DOUBLE);
                        }

                        if (lon != null) {
                            ps.setDouble(7, lon);
                            ps.setDouble(8, lon - 0.01);
                            ps.setDouble(9, lon + 0.01);
                        } else {
                            ps.setNull(7, java.sql.Types.DOUBLE);
                            ps.setNull(8, java.sql.Types.DOUBLE);
                            ps.setNull(9, java.sql.Types.DOUBLE);
                        }

                        try (ResultSet rs = ps.executeQuery()) {
                            if (rs.next()) {
                                out.put("source_type", rs.getString("source_type"));
                                out.put("source_id", rs.getString("source_id"));
                                api.putNullable(out, "lat", rs.getObject("lat"));
                                api.putNullable(out, "lon", rs.getObject("lon"));
                                out.put("as_of_date", String.valueOf(rs.getObject("as_of_date")));
                                api.putNullable(out, "horizon_hours", rs.getObject("horizon_hours"));
                                api.putNullable(out, "tmean_c", rs.getObject("tmean_c"));
                                api.putNullable(out, "prcp_mm", rs.getObject("prcp_mm"));
                                api.putNullable(out, "tmin_c", rs.getObject("tmin_c"));
                                api.putNullable(out, "tmax_c", rs.getObject("tmax_c"));
                                api.putNullable(out, "delta_c", rs.getObject("delta_c"));
                                out.put("model_name", rs.getString("model_name"));
                                out.put("model_detail", rs.getString("model_detail"));
                                api.putNullable(out, "confidence", rs.getObject("confidence"));
                                out.put("created_at", String.valueOf(rs.getObject("created_at")));
                                ctx.json(out);
                                return;
                            }
                        }
                    }
                }
            } catch (Exception e) {
                log.warn("ML weather latest query failed: {}", e.getMessage());
            }

            ctx.status(404).json(om.createObjectNode().put("error", "no predictions"));
        });

        app.get("/api/ml/weather/forecast", ctx -> {
            String sourceType = ctx.queryParam("sourceType");
            String sourceId = ctx.queryParam("sourceId");
            Double lat = ApiServer.parseDouble(ctx.queryParam("lat"), null);
            Double lon = ApiServer.parseDouble(ctx.queryParam("lon"), null);
            int days = ApiServer.parseInt(ctx.queryParam("days"), 10, 1, 10);

            if (sourceType == null || sourceType.isBlank()) {
                ctx.status(400).json(om.createObjectNode().put("error", "sourceType is required"));
                return;
            }

            String effectiveSourceType = sourceType;
            if ("point".equals(sourceType)) {
                effectiveSourceType = "gridpoint";
            }

            if (sourceId != null && !sourceId.isBlank()) {
                lat = null;
                lon = null;
            }

            String sql = """
                            SELECT as_of_date, horizon_hours, tmean_c, prcp_mm, tmin_c, tmax_c, delta_c,
                                         model_name, model_detail, confidence, created_at
                            FROM ml_weather_prediction
                            WHERE source_type = ?
                                AND (? IS NULL OR source_id = ?)
                                AND (? IS NULL OR lat BETWEEN ? AND ?)
                                AND (? IS NULL OR lon BETWEEN ? AND ?)
                                AND horizon_hours BETWEEN 0 AND ((? - 1) * 24)
                            ORDER BY horizon_hours ASC
                    """;

            ArrayNode arr = om.createArrayNode();
            try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
                int i = 1;
                ps.setString(i++, effectiveSourceType);
                ps.setString(i++, sourceId);
                ps.setString(i++, sourceId);

                if (lat != null) {
                    ps.setDouble(i++, lat);
                    ps.setDouble(i++, lat - 0.01);
                    ps.setDouble(i++, lat + 0.01);
                } else {
                    ps.setNull(i++, java.sql.Types.DOUBLE);
                    ps.setNull(i++, java.sql.Types.DOUBLE);
                    ps.setNull(i++, java.sql.Types.DOUBLE);
                }

                if (lon != null) {
                    ps.setDouble(i++, lon);
                    ps.setDouble(i++, lon - 0.01);
                    ps.setDouble(i++, lon + 0.01);
                } else {
                    ps.setNull(i++, java.sql.Types.DOUBLE);
                    ps.setNull(i++, java.sql.Types.DOUBLE);
                    ps.setNull(i++, java.sql.Types.DOUBLE);
                }

                ps.setInt(i, days);

                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        ObjectNode row = om.createObjectNode();
                        row.put("as_of_date", String.valueOf(rs.getObject("as_of_date")));
                        row.put("horizon_hours", rs.getInt("horizon_hours"));
                        api.putNullable(row, "tmean_c", rs.getObject("tmean_c"));
                        api.putNullable(row, "prcp_mm", rs.getObject("prcp_mm"));
                        api.putNullable(row, "tmin_c", rs.getObject("tmin_c"));
                        api.putNullable(row, "tmax_c", rs.getObject("tmax_c"));
                        api.putNullable(row, "delta_c", rs.getObject("delta_c"));
                        row.put("model_name", rs.getString("model_name"));
                        row.put("model_detail", rs.getString("model_detail"));
                        api.putNullable(row, "confidence", rs.getObject("confidence"));
                        row.put("created_at", String.valueOf(rs.getObject("created_at")));
                        arr.add(row);
                    }
                }
            }

            ctx.json(arr);
        });
    }
}
