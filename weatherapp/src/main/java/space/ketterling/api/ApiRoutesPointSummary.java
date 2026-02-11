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
 * Routes that summarize data for a single lat/lon point.
 */
final class ApiRoutesPointSummary {
    /**
     * Utility class; do not instantiate.
     */
    private ApiRoutesPointSummary() {
    }

    /**
     * Registers the point summary endpoint.
     */
    static void register(ApiServer api) {
        Javalin app = api.app();
        ObjectMapper om = api.om();
        HikariDataSource ds = api.ds();

        app.get("/api/point/summary", ctx -> {
            Double lat = null;
            Double lon = null;
            try {
                lat = Double.parseDouble(ctx.queryParam("lat"));
                lon = Double.parseDouble(ctx.queryParam("lon"));
            } catch (Exception ignored) {
            }

            if (lat == null || lon == null) {
                ctx.status(400).json(om.createObjectNode().put("error", "lat and lon are required"));
                return;
            }
            if (!ApiServer.isLatLonValid(lat, lon)) {
                ctx.status(400).json(om.createObjectNode().put("error", "lat or lon out of range"));
                return;
            }

            int days = ApiServer.parseInt(ctx.queryParam("days"), 30, 1, 3650);
            int limit = ApiServer.parseInt(ctx.queryParam("limit"), 10, 1, 50);

            String cacheKey = String.format("pointSummary:%.4f,%.4f:%d:%d", ApiServer.roundTo(lat, 4),
                    ApiServer.roundTo(lon, 4), days,
                    limit);
            if (api.serveCached(ctx, cacheKey)) {
                return;
            }

            ObjectNode out = om.createObjectNode();
            ObjectNode q = om.createObjectNode();
            q.put("lat", lat);
            q.put("lon", lon);
            q.put("days", days);
            out.set("query", q);

            ArrayNode stations = om.createArrayNode();
            Double interpTmean = null;
            Double interpPrcp = null;
            Double weightSumT = 0.0;
            Double weightSumP = 0.0;
            Double tmeanWeighted = 0.0;
            Double prcpWeighted = 0.0;

            String stationSql = """
                    WITH nearest AS (
                        SELECT station_id, name,
                               ST_Y(geom) AS lat,
                               ST_X(geom) AS lon,
                               ST_Distance(geom::geography, ST_SetSRID(ST_MakePoint(?, ?), 4326)::geography) AS dist_m
                        FROM noaa_station
                        WHERE geom IS NOT NULL
                        ORDER BY geom <-> ST_SetSRID(ST_MakePoint(?, ?), 4326)
                        LIMIT ?
                    ), daily AS (
                        SELECT n.*,
                               d.date AS latest_date,
                               d.tmax_c, d.tmin_c, d.prcp_mm AS prcp_latest_mm,
                               p.prcp_window_mm,
                               cov.rows_total, cov.rows_tmax, cov.rows_tmin, cov.rows_prcp,
                               cov.first_date, cov.last_date
                        FROM nearest n
                        LEFT JOIN LATERAL (
                            SELECT date, tmax_c, tmin_c, prcp_mm
                            FROM noaa_daily_summary
                            WHERE station_id = n.station_id
                            ORDER BY date DESC
                            LIMIT 1
                        ) d ON true
                        LEFT JOIN LATERAL (
                            SELECT SUM(prcp_mm) AS prcp_window_mm
                            FROM noaa_daily_summary
                            WHERE station_id = n.station_id
                              AND date >= current_date - (? * INTERVAL '1 day')
                              AND date < current_date
                        ) p ON true
                        LEFT JOIN LATERAL (
                            SELECT COUNT(*) AS rows_total,
                                   COUNT(tmax_c) AS rows_tmax,
                                   COUNT(tmin_c) AS rows_tmin,
                                   COUNT(prcp_mm) AS rows_prcp,
                                   MIN(date) AS first_date,
                                   MAX(date) AS last_date
                            FROM noaa_daily_summary
                            WHERE station_id = n.station_id
                        ) cov ON true
                    )
                    SELECT * FROM daily
                    """;

            try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(stationSql)) {
                ps.setDouble(1, lon);
                ps.setDouble(2, lat);
                ps.setDouble(3, lon);
                ps.setDouble(4, lat);
                ps.setInt(5, limit);
                ps.setInt(6, days);

                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        ObjectNode row = om.createObjectNode();
                        String stationId = rs.getString("station_id");
                        row.put("station_id", stationId);
                        row.put("name", rs.getString("name"));
                        api.putNullable(row, "lat", rs.getObject("lat"));
                        api.putNullable(row, "lon", rs.getObject("lon"));
                        double distM = rs.getDouble("dist_m");
                        row.put("dist_km", distM / 1000.0);
                        row.put("latest_date", String.valueOf(rs.getObject("latest_date")));
                        api.putNullable(row, "tmax_c", rs.getObject("tmax_c"));
                        api.putNullable(row, "tmin_c", rs.getObject("tmin_c"));
                        api.putNullable(row, "prcp_latest_mm", rs.getObject("prcp_latest_mm"));
                        api.putNullable(row, "prcp_window_mm", rs.getObject("prcp_window_mm"));
                        api.putNullable(row, "rows_total", rs.getObject("rows_total"));
                        api.putNullable(row, "rows_tmax", rs.getObject("rows_tmax"));
                        api.putNullable(row, "rows_tmin", rs.getObject("rows_tmin"));
                        api.putNullable(row, "rows_prcp", rs.getObject("rows_prcp"));
                        api.putNullable(row, "first_date", rs.getObject("first_date"));
                        api.putNullable(row, "last_date", rs.getObject("last_date"));
                        if (rs.getObject("tmax_c") != null && rs.getObject("tmin_c") != null) {
                            double tmean = (rs.getDouble("tmax_c") + rs.getDouble("tmin_c")) / 2.0;
                            row.put("tmean_c", tmean);
                        }
                        stations.add(row);

                        Double tmax = rs.getObject("tmax_c") == null ? null : rs.getDouble("tmax_c");
                        Double tmin = rs.getObject("tmin_c") == null ? null : rs.getDouble("tmin_c");
                        Double prcpWindow = rs.getObject("prcp_window_mm") == null ? null
                                : rs.getDouble("prcp_window_mm");

                        double w = 1.0 / Math.max(distM, 1.0);
                        if (tmax != null && tmin != null) {
                            double tmean = (tmax + tmin) / 2.0;
                            tmeanWeighted += w * tmean;
                            weightSumT += w;
                        }
                        if (prcpWindow != null) {
                            prcpWeighted += w * prcpWindow;
                            weightSumP += w;
                        }
                    }
                }
            }

            if (weightSumT > 0) {
                interpTmean = tmeanWeighted / weightSumT;
            }
            if (weightSumP > 0) {
                interpPrcp = prcpWeighted / weightSumP;
            }

            out.set("nearest_stations", stations);
            ObjectNode interpolated = om.createObjectNode();
            interpolated.put("days", days);
            api.putNullable(interpolated, "tmean_c", interpTmean);
            api.putNullable(interpolated, "prcp_window_mm", interpPrcp);
            out.set("interpolated", interpolated);

            String gridSql = "SELECT grid_id, office, grid_x, grid_y, ST_Y(geom) AS lat, ST_X(geom) AS lon " +
                    "FROM geo_gridpoint WHERE geom IS NOT NULL " +
                    "ORDER BY geom <-> ST_SetSRID(ST_MakePoint(?, ?), 4326) LIMIT 1";

            ObjectNode nearestGrid = null;
            try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(gridSql)) {
                ps.setDouble(1, lon);
                ps.setDouble(2, lat);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        nearestGrid = om.createObjectNode();
                        nearestGrid.put("grid_id", rs.getString("grid_id"));
                        nearestGrid.put("office", rs.getString("office"));
                        nearestGrid.put("grid_x", rs.getInt("grid_x"));
                        nearestGrid.put("grid_y", rs.getInt("grid_y"));
                        api.putNullable(nearestGrid, "lat", rs.getObject("lat"));
                        api.putNullable(nearestGrid, "lon", rs.getObject("lon"));
                    }
                }
            }

            if (nearestGrid != null) {
                out.set("nearest_gridpoint", nearestGrid);

                String hourlySql = """
                        SELECT start_time, end_time, temperature_c, wind_speed_mps, wind_gust_mps, wind_dir_deg,
                               precip_prob, relative_humidity, short_forecast
                        FROM v_latest_hourly_forecast
                        WHERE grid_id = ?
                        ORDER BY start_time ASC
                        LIMIT 1
                        """;

                ObjectNode hourly = null;
                try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(hourlySql)) {
                    ps.setString(1, nearestGrid.get("grid_id").asText());
                    try (ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            hourly = om.createObjectNode();
                            hourly.put("start_time", String.valueOf(rs.getObject("start_time")));
                            hourly.put("end_time", String.valueOf(rs.getObject("end_time")));
                            api.putNullable(hourly, "temperature_c", rs.getObject("temperature_c"));
                            api.putNullable(hourly, "wind_speed_mps", rs.getObject("wind_speed_mps"));
                            api.putNullable(hourly, "wind_gust_mps", rs.getObject("wind_gust_mps"));
                            api.putNullable(hourly, "wind_dir_deg", rs.getObject("wind_dir_deg"));
                            api.putNullable(hourly, "precip_prob", rs.getObject("precip_prob"));
                            api.putNullable(hourly, "relative_humidity", rs.getObject("relative_humidity"));
                            hourly.put("short_forecast", rs.getString("short_forecast"));
                        }
                    }
                }

                if (hourly != null) {
                    out.set("hourly", hourly);
                }
            }

            try {
                api.cacheAndRespond(ctx, cacheKey, out, 300, 600);
            } catch (Exception e) {
                ctx.json(out);
            }
        });
    }
}
