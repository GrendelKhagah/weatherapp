package space.ketterling.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.zaxxer.hikari.HikariDataSource;
import io.javalin.Javalin;
import space.ketterling.db.GridpointRepo;
import space.ketterling.nws.NwsClient;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * Routes that return hourly and daily forecast data.
 * These endpoints power the forecast widgets in the UI.
 */
final class ApiRoutesForecast {
    /**
     * Utility class; do not instantiate.
     */
    private ApiRoutesForecast() {
    }

    /**
     * Registers forecast-related HTTP endpoints.
     */
    static void register(ApiServer api) {
        Javalin app = api.app();
        ObjectMapper om = api.om();
        HikariDataSource ds = api.ds();
        NwsClient nws = api.nws();
        GridpointRepo gridpointRepo = api.gridpointRepo();

        app.get("/api/forecast/hourly", ctx -> {
            String gridId = ApiServer.firstNonBlank(ctx.queryParam("gridId"), ctx.queryParam("grid_id"));
            if (gridId == null || gridId.isBlank()) {
                ctx.status(400).json(om.createObjectNode().put("error", "gridId (or grid_id) is required"));
                return;
            }

            Integer limit = ApiServer.parseInt(ctx.queryParam("limit"), 96, 1, 240);
            Integer hours = ApiServer.parseInt(ctx.queryParam("hours"), null, 1, 168);

            String startIso = ctx.queryParam("start");
            String endIso = ctx.queryParam("end");

            StringBuilder sql = new StringBuilder("""
                        SELECT start_time, end_time, temperature_c, wind_speed_mps, wind_gust_mps, wind_dir_deg,
                               precip_prob, relative_humidity, short_forecast, issued_at, ingested_at
                        FROM v_latest_hourly_forecast
                        WHERE grid_id=?
                    """);

            if (hours != null) {
                sql.append(" AND start_time >= now() AND start_time < now() + (? || ' hours')::interval ");
            }
            if (startIso != null && !startIso.isBlank()) {
                sql.append(" AND start_time >= ?::timestamptz ");
            }
            if (endIso != null && !endIso.isBlank()) {
                sql.append(" AND start_time < ?::timestamptz ");
            }

            sql.append(" ORDER BY start_time ");

            if (hours == null) {
                sql.append(" LIMIT ? ");
            }

            ArrayNode arr = om.createArrayNode();
            try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql.toString())) {
                int i = 1;
                ps.setString(i++, gridId);

                if (hours != null) {
                    ps.setInt(i++, hours);
                }
                if (startIso != null && !startIso.isBlank()) {
                    ps.setString(i++, startIso);
                }
                if (endIso != null && !endIso.isBlank()) {
                    ps.setString(i++, endIso);
                }
                if (hours == null) {
                    ps.setInt(i, limit);
                }

                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        ObjectNode row = om.createObjectNode();
                        row.put("start_time", rs.getString("start_time"));
                        row.put("end_time", rs.getString("end_time"));
                        api.putNullable(row, "temperature_c", rs.getObject("temperature_c"));
                        api.putNullable(row, "wind_speed_mps", rs.getObject("wind_speed_mps"));
                        api.putNullable(row, "wind_gust_mps", rs.getObject("wind_gust_mps"));
                        api.putNullable(row, "wind_dir_deg", rs.getObject("wind_dir_deg"));
                        api.putNullable(row, "precip_prob", rs.getObject("precip_prob"));
                        api.putNullable(row, "relative_humidity", rs.getObject("relative_humidity"));
                        row.put("short_forecast", rs.getString("short_forecast"));
                        row.put("issued_at", String.valueOf(rs.getObject("issued_at")));
                        row.put("ingested_at", String.valueOf(rs.getObject("ingested_at")));
                        arr.add(row);
                    }
                }
            }

            ctx.json(arr);
        });

        app.get("/api/forecast/daily", ctx -> {
            String gridId = ApiServer.firstNonBlank(ctx.queryParam("gridId"), ctx.queryParam("grid_id"));
            if (gridId == null || gridId.isBlank()) {
                ctx.status(400).json(om.createObjectNode().put("error", "gridId (or grid_id) is required"));
                return;
            }

            int days = ApiServer.parseInt(ctx.queryParam("days"), 10, 1, 14);

            String sql = """
                        SELECT date_trunc('day', start_time) AS day,
                               MIN(temperature_c) AS tmin_c,
                               MAX(temperature_c) AS tmax_c,
                               AVG(precip_prob) AS precip_prob
                        FROM v_latest_hourly_forecast
                        WHERE grid_id = ?
                          AND start_time >= now()
                          AND start_time < now() + (? || ' days')::interval
                        GROUP BY 1
                        ORDER BY 1
                    """;

            ArrayNode arr = om.createArrayNode();
            try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
                ps.setString(1, gridId);
                ps.setInt(2, days);

                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        ObjectNode row = om.createObjectNode();
                        row.put("day", String.valueOf(rs.getObject("day")));
                        api.putNullable(row, "tmin_c", rs.getObject("tmin_c"));
                        api.putNullable(row, "tmax_c", rs.getObject("tmax_c"));
                        api.putNullable(row, "precip_prob", rs.getObject("precip_prob"));
                        arr.add(row);
                    }
                }
            }

            ctx.json(arr);
        });

        app.get("/api/forecast/hourly/point", ctx -> {
            if (nws == null) {
                ctx.status(503).json(om.createObjectNode().put("error", "NWS client unavailable"));
                return;
            }

            Double lat = ApiServer.parseDouble(ctx.queryParam("lat"), null);
            Double lon = ApiServer.parseDouble(ctx.queryParam("lon"), null);
            Integer limit = ApiServer.parseInt(ctx.queryParam("limit"), 24, 1, 168);
            String mode = ctx.queryParam("mode");
            String refreshParam = ctx.queryParam("refresh");
            boolean forceRefresh = refreshParam != null &&
                    (refreshParam.equals("1") || refreshParam.equalsIgnoreCase("true")
                            || refreshParam.equalsIgnoreCase("yes"));
            if (lat == null || lon == null) {
                ctx.status(400).json(om.createObjectNode().put("error", "lat and lon are required"));
                return;
            }
            if (!ApiServer.isLatLonValid(lat, lon)) {
                ctx.status(400).json(om.createObjectNode().put("error", "lat or lon out of range"));
                return;
            }

            String cacheKey = String.format("hourlyPoint:%.4f,%.4f:%d:%s", ApiServer.roundTo(lat, 4),
                    ApiServer.roundTo(lon, 4),
                    limit, mode == null ? "" : mode.toLowerCase());
            if (!forceRefresh && api.serveCached(ctx, cacheKey)) {
                return;
            }

            Double nearestDistM = null;
            String nearestHourlyUrl = null;
            String nearestOffice = null;
            String nearestGridId = null;

            String nearestSql = """
                    SELECT grid_id, office, grid_x, grid_y, forecast_hourly_url,
                           ST_Distance(geom::geography, ST_SetSRID(ST_MakePoint(?, ?), 4326)::geography) AS dist_m
                    FROM geo_gridpoint
                    WHERE geom IS NOT NULL
                    ORDER BY geom <-> ST_SetSRID(ST_MakePoint(?, ?), 4326)
                    LIMIT 1
                    """;

            try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(nearestSql)) {
                ps.setDouble(1, lon);
                ps.setDouble(2, lat);
                ps.setDouble(3, lon);
                ps.setDouble(4, lat);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        nearestGridId = rs.getString("grid_id");
                        nearestOffice = rs.getString("office");
                        nearestHourlyUrl = rs.getString("forecast_hourly_url");
                        nearestDistM = rs.getDouble("dist_m");
                    }
                }
            }

            double maxDistM = 274.32; // 900 ft

            if (nearestGridId != null && nearestDistM != null && nearestDistM <= maxDistM) {
                ObjectNode out = om.createObjectNode();
                out.put("lat", lat);
                out.put("lon", lon);
                out.put("grid_id", nearestGridId);
                out.put("office", nearestOffice);
                out.put("source", "db");

                ArrayNode list = api.fetchHourlyFromDb(nearestGridId, limit);
                if (list != null && list.size() > 0) {
                    if (mode != null && mode.equalsIgnoreCase("list")) {
                        out.set("periods", list);
                    } else {
                        out.set("hourly", list.get(0));
                    }
                    try {
                        api.cacheAndRespond(ctx, cacheKey, out, 300, 600);
                    } catch (Exception e) {
                        ctx.json(out);
                    }
                    return;
                }

                if (nearestHourlyUrl != null && !nearestHourlyUrl.isBlank()) {
                    ObjectNode outNws = api.fetchAndStoreHourlyFromNws(nearestGridId, nearestHourlyUrl, out);
                    try {
                        api.cacheAndRespond(ctx, cacheKey, outNws, 300, 600);
                    } catch (Exception e) {
                        ctx.json(outNws);
                    }
                    return;
                }
            }

            JsonNode point = nws.points(lat, lon);
            JsonNode props = point.path("properties");
            String gridId = props.path("gridId").asText(null);
            int gridX = props.path("gridX").asInt();
            int gridY = props.path("gridY").asInt();
            String gridDataUrl = props.path("forecastGridData").asText(null);
            String forecastHourlyUrl = props.path("forecastHourly").asText(null);
            if (forecastHourlyUrl == null || forecastHourlyUrl.isBlank() || gridId == null) {
                ctx.status(503).json(om.createObjectNode().put("error", "Missing forecastHourly URL"));
                return;
            }

            String fullGridId = gridId + ":" + gridX + "," + gridY;
            GridpointRepo.GridpointDetail existing = gridpointRepo.getGridpointById(fullGridId);

            ObjectNode out = om.createObjectNode();
            out.put("lat", lat);
            out.put("lon", lon);
            out.put("grid_id", fullGridId);
            out.put("office", gridId);

            if (existing != null) {
                out.put("source", "db");
                ArrayNode list = api.fetchHourlyFromDb(fullGridId, limit);
                if (list != null && list.size() > 0) {
                    if (mode != null && mode.equalsIgnoreCase("list")) {
                        out.set("periods", list);
                    } else {
                        out.set("hourly", list.get(0));
                    }
                    try {
                        api.cacheAndRespond(ctx, cacheKey, out, 300, 600);
                    } catch (Exception e) {
                        ctx.json(out);
                    }
                    return;
                }

                String existingHourlyUrl = existing.forecastHourlyUrl();
                if (existingHourlyUrl != null && !existingHourlyUrl.isBlank()) {
                    ObjectNode outNws = api.fetchAndStoreHourlyFromNws(fullGridId, existingHourlyUrl, out);
                    try {
                        api.cacheAndRespond(ctx, cacheKey, outNws, 300, 600);
                    } catch (Exception e) {
                        ctx.json(outNws);
                    }
                    return;
                }
            }

            gridpointRepo.upsertGridpoint(fullGridId, gridId, gridX, gridY, lat, lon, gridDataUrl, forecastHourlyUrl);
            out.put("source", "nws");

            ObjectNode outNws = api.fetchAndStoreHourlyFromNws(fullGridId, forecastHourlyUrl, out);
            try {
                api.cacheAndRespond(ctx, cacheKey, outNws, 300, 600);
            } catch (Exception e) {
                ctx.json(outNws);
            }
        });
    }
}
