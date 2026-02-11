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
 * Routes that provide historic (past) daily weather data.
 */
final class ApiRoutesHistory {
    /**
     * Utility class; do not instantiate.
     */
    private ApiRoutesHistory() {
    }

    /**
     * Registers history endpoints (daily and gridpoint history).
     */
    static void register(ApiServer api) {
        Javalin app = api.app();
        ObjectMapper om = api.om();
        HikariDataSource ds = api.ds();

        app.get("/api/history/daily", ctx -> {
            String stationId = ctx.queryParam("stationId");
            String start = ctx.queryParam("start");
            String end = ctx.queryParam("end");
            if (stationId == null || stationId.isBlank()) {
                ctx.status(400).json(om.createObjectNode().put("error", "stationId is required"));
                return;
            }

            if (start == null || start.isBlank() || end == null || end.isBlank()) {
                ctx.status(400).json(om.createObjectNode().put("error", "start and end dates are required"));
                return;
            }

            String cacheKey = String.format("historyDaily:%s:%s:%s", stationId, start, end);
            if (api.serveCached(ctx, cacheKey)) {
                return;
            }

            String sql = "SELECT date, tmax_c, tmin_c, prcp_mm FROM noaa_daily_summary WHERE station_id = ? AND date >= ?::date AND date <= ?::date ORDER BY date";
            ArrayNode arr = om.createArrayNode();
            try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
                ps.setString(1, stationId);
                ps.setString(2, start);
                ps.setString(3, end);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        ObjectNode row = om.createObjectNode();
                        row.put("date", String.valueOf(rs.getObject("date")));
                        api.putNullable(row, "tmax_c", rs.getObject("tmax_c"));
                        api.putNullable(row, "tmin_c", rs.getObject("tmin_c"));
                        api.putNullable(row, "prcp_mm", rs.getObject("prcp_mm"));
                        arr.add(row);
                    }
                }
            }

            try {
                api.cacheAndRespond(ctx, cacheKey, arr, 21600, 43200);
            } catch (Exception e) {
                ctx.json(arr);
            }
        });

        app.get("/api/history/gridpoint", ctx -> {
            String gridId = ctx.queryParam("gridId");
            int days = ApiServer.parseInt(ctx.queryParam("days"), 365, 1, 3650);
            if (gridId == null || gridId.isBlank()) {
                ctx.status(400).json(om.createObjectNode().put("error", "gridId is required"));
                return;
            }

            String cacheKey = String.format("historyGridpoint:%s:%d", gridId, days);
            if (api.serveCached(ctx, cacheKey)) {
                return;
            }

            String stationSql = "SELECT station_id FROM gridpoint_station_map WHERE grid_id = ? AND is_primary = true LIMIT 1";
            String stationId = null;
            try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(stationSql)) {
                ps.setString(1, gridId);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next())
                        stationId = rs.getString(1);
                }
            }

            if (stationId == null) {
                ctx.status(404).json(om.createObjectNode().put("error", "no primary station for gridId"));
                return;
            }

            java.time.LocalDate endDate = java.time.LocalDate.now().minusDays(1);
            java.time.LocalDate startDate = endDate.minusDays(days - 1);

            String sql = "SELECT date, tmax_c, tmin_c, prcp_mm FROM noaa_daily_summary WHERE station_id = ? AND date >= ? AND date <= ? ORDER BY date";
            ArrayNode arr = om.createArrayNode();
            try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
                ps.setString(1, stationId);
                ps.setObject(2, startDate);
                ps.setObject(3, endDate);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        ObjectNode row = om.createObjectNode();
                        row.put("date", String.valueOf(rs.getObject("date")));
                        api.putNullable(row, "tmax_c", rs.getObject("tmax_c"));
                        api.putNullable(row, "tmin_c", rs.getObject("tmin_c"));
                        api.putNullable(row, "prcp_mm", rs.getObject("prcp_mm"));
                        arr.add(row);
                    }
                }
            }

            try {
                api.cacheAndRespond(ctx, cacheKey, arr, 3600, 7200);
            } catch (Exception e) {
                ctx.json(arr);
            }
        });
    }
}
