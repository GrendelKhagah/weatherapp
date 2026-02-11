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
 * Routes that return map layers like gridpoints, stations, and alerts.
 */
final class ApiRoutesGeo {
    private static final Logger log = LoggerFactory.getLogger(ApiRoutesGeo.class);

    /**
     * Utility class; do not instantiate.
     */
    private ApiRoutesGeo() {
    }

    /**
     * Registers the map/geo endpoints.
     */
    static void register(ApiServer api) {
        Javalin app = api.app();
        ObjectMapper om = api.om();
        HikariDataSource ds = api.ds();

        app.get("/api/gridpoints", ctx -> {
            log.info("GET /api/gridpoints bbox={}", ctx.queryParam("bbox"));
            double[] bbox = api.parseBbox(ctx.queryParam("bbox"));
            String cacheKey = "gridpoints:" + (bbox == null ? "all" : ApiServer.bboxKey(bbox));
            if (api.serveCached(ctx, cacheKey)) {
                return;
            }
            ObjectNode fc = api.featureCollection();

            String sql = "SELECT grid_id, office, grid_x, grid_y, ST_AsGeoJSON(geom) AS geom " +
                    "FROM geo_gridpoint " +
                    (bbox != null ? "WHERE ST_Intersects(geom, ST_MakeEnvelope(?, ?, ?, ?, 4326)) " : "") +
                    "ORDER BY grid_id";

            try (Connection c = ds.getConnection();
                    PreparedStatement ps = c.prepareStatement(sql)) {

                if (bbox != null) {
                    ps.setDouble(1, bbox[0]);
                    ps.setDouble(2, bbox[1]);
                    ps.setDouble(3, bbox[2]);
                    ps.setDouble(4, bbox[3]);
                }

                try (ResultSet rs = ps.executeQuery()) {
                    ArrayNode feats = (ArrayNode) fc.get("features");
                    while (rs.next()) {
                        ObjectNode feat = om.createObjectNode();
                        feat.put("type", "Feature");

                        ObjectNode props = om.createObjectNode();
                        props.put("grid_id", rs.getString("grid_id"));
                        props.put("office", rs.getString("office"));
                        props.put("grid_x", rs.getInt("grid_x"));
                        props.put("grid_y", rs.getInt("grid_y"));
                        feat.set("properties", props);

                        String geomJson = rs.getString("geom");
                        feat.set("geometry", geomJson == null ? om.nullNode() : om.readTree(geomJson));

                        feats.add(feat);
                    }
                }
            }

            try {
                api.cacheAndRespond(ctx, cacheKey, fc, 30, 120);
            } catch (Exception e) {
                ctx.json(fc);
            }
        });

        app.get("/api/alerts", ctx -> {
            log.info("GET /api/alerts bbox={}", ctx.queryParam("bbox"));
            double[] bbox = api.parseBbox(ctx.queryParam("bbox"));
            String cacheKey = "alerts:" + (bbox == null ? "all" : ApiServer.bboxKey(bbox));
            if (api.serveCached(ctx, cacheKey)) {
                return;
            }
            ObjectNode fc = api.featureCollection();

            String sql = "SELECT alert_id, event, severity, urgency, headline, expires, ST_AsGeoJSON(geom) AS geom " +
                    "FROM v_active_alerts " +
                    "WHERE geom IS NOT NULL " +
                    (bbox != null ? "AND ST_Intersects(geom, ST_MakeEnvelope(?, ?, ?, ?, 4326)) " : "") +
                    "ORDER BY expires NULLS LAST";

            try (Connection c = ds.getConnection();
                    PreparedStatement ps = c.prepareStatement(sql)) {

                if (bbox != null) {
                    ps.setDouble(1, bbox[0]);
                    ps.setDouble(2, bbox[1]);
                    ps.setDouble(3, bbox[2]);
                    ps.setDouble(4, bbox[3]);
                }

                try (ResultSet rs = ps.executeQuery()) {
                    ArrayNode feats = (ArrayNode) fc.get("features");
                    while (rs.next()) {
                        ObjectNode feat = om.createObjectNode();
                        feat.put("type", "Feature");

                        ObjectNode props = om.createObjectNode();
                        props.put("alert_id", rs.getString("alert_id"));
                        props.put("event", rs.getString("event"));
                        props.put("severity", rs.getString("severity"));
                        props.put("urgency", rs.getString("urgency"));
                        props.put("headline", rs.getString("headline"));
                        props.put("expires", rs.getString("expires"));
                        feat.set("properties", props);

                        String geomJson = rs.getString("geom");
                        feat.set("geometry", geomJson == null ? om.nullNode() : om.readTree(geomJson));

                        feats.add(feat);
                    }
                }
            }

            try {
                api.cacheAndRespond(ctx, cacheKey, fc, 30, 120);
            } catch (Exception e) {
                ctx.json(fc);
            }
        });

        app.get("/api/stations/near", ctx -> {
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
            int limit = ApiServer.parseInt(ctx.queryParam("limit"), 10, 1, 50);

            String cacheKey = String.format("stations:%.4f,%.4f:%d", ApiServer.roundTo(lat, 4),
                    ApiServer.roundTo(lon, 4), limit);
            if (api.serveCached(ctx, cacheKey)) {
                return;
            }

            ObjectNode fc = api.featureCollection();
            String sql = """
                    WITH nearest AS (
                        SELECT station_id, name, geom,
                               ST_Distance(geom::geography, ST_SetSRID(ST_MakePoint(?, ?), 4326)::geography) AS dist_m
                        FROM noaa_station
                        WHERE geom IS NOT NULL
                        ORDER BY geom <-> ST_SetSRID(ST_MakePoint(?, ?), 4326)
                        LIMIT ?
                    ), daily AS (
                        SELECT n.*, d.date AS latest_date, d.tmax_c, d.tmin_c, d.prcp_mm,
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
                    SELECT station_id, name, dist_m, latest_date, tmax_c, tmin_c, prcp_mm,
                           rows_total, rows_tmax, rows_tmin, rows_prcp, first_date, last_date,
                           ST_AsGeoJSON(geom) AS geom
                    FROM daily
                    """;

            try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
                ps.setDouble(1, lon);
                ps.setDouble(2, lat);
                ps.setDouble(3, lon);
                ps.setDouble(4, lat);
                ps.setInt(5, limit);

                try (ResultSet rs = ps.executeQuery()) {
                    ArrayNode feats = (ArrayNode) fc.get("features");
                    while (rs.next()) {
                        ObjectNode feat = om.createObjectNode();
                        feat.put("type", "Feature");

                        ObjectNode props = om.createObjectNode();
                        props.put("station_id", rs.getString("station_id"));
                        props.put("name", rs.getString("name"));
                        props.put("dist_km", rs.getDouble("dist_m") / 1000.0);
                        props.put("latest_date", String.valueOf(rs.getObject("latest_date")));
                        api.putNullable(props, "tmax_c", rs.getObject("tmax_c"));
                        api.putNullable(props, "tmin_c", rs.getObject("tmin_c"));
                        api.putNullable(props, "prcp_mm", rs.getObject("prcp_mm"));
                        api.putNullable(props, "rows_total", rs.getObject("rows_total"));
                        api.putNullable(props, "rows_tmax", rs.getObject("rows_tmax"));
                        api.putNullable(props, "rows_tmin", rs.getObject("rows_tmin"));
                        api.putNullable(props, "rows_prcp", rs.getObject("rows_prcp"));
                        api.putNullable(props, "first_date", rs.getObject("first_date"));
                        api.putNullable(props, "last_date", rs.getObject("last_date"));
                        if (rs.getObject("tmax_c") != null && rs.getObject("tmin_c") != null) {
                            double tmean = (rs.getDouble("tmax_c") + rs.getDouble("tmin_c")) / 2.0;
                            props.put("tmean_c", tmean);
                        }
                        feat.set("properties", props);

                        String geomJson = rs.getString("geom");
                        feat.set("geometry", geomJson == null ? om.nullNode() : om.readTree(geomJson));

                        feats.add(feat);
                    }
                }
            }

            try {
                api.cacheAndRespond(ctx, cacheKey, fc, 300, 600);
            } catch (Exception e) {
                ctx.json(fc);
            }
        });

        app.get("/api/stations/all", ctx -> {
            double[] bbox = api.parseBbox(ctx.queryParam("bbox"));
            if (bbox == null) {
                ctx.status(400).json(om.createObjectNode().put("error", "bbox is required"));
                return;
            }
            int limit = ApiServer.parseInt(ctx.queryParam("limit"), 5000, 1, 20000);
            String withDataParam = ctx.queryParam("withData");
            boolean withData = withDataParam != null && ("1".equals(withDataParam)
                    || "true".equalsIgnoreCase(withDataParam)
                    || "yes".equalsIgnoreCase(withDataParam));

            String cacheKey = "stations-all:" + ApiServer.bboxKey(bbox) + ":" + limit + ":"
                    + (withData ? "data" : "all");
            if (api.serveCached(ctx, cacheKey)) {
                return;
            }

            ObjectNode fc = api.featureCollection();
            String sql = """
                        SELECT s.station_id, s.name, d.date AS latest_date, d.tmax_c, d.tmin_c, d.prcp_mm,
                               ST_AsGeoJSON(s.geom) AS geom
                        FROM noaa_station s
                        LEFT JOIN LATERAL (
                            SELECT date, tmax_c, tmin_c, prcp_mm
                            FROM noaa_daily_summary
                            WHERE station_id = s.station_id
                            ORDER BY date DESC
                            LIMIT 1
                        ) d ON true
                        WHERE s.geom IS NOT NULL
                          AND ST_Intersects(s.geom, ST_MakeEnvelope(?, ?, ?, ?, 4326))
                          AND (? = false OR d.date IS NOT NULL)
                        ORDER BY (d.date IS NULL) ASC, d.date DESC
                        LIMIT ?
                    """;

            try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
                ps.setDouble(1, bbox[0]);
                ps.setDouble(2, bbox[1]);
                ps.setDouble(3, bbox[2]);
                ps.setDouble(4, bbox[3]);
                ps.setBoolean(5, withData);
                ps.setInt(6, limit);

                try (ResultSet rs = ps.executeQuery()) {
                    ArrayNode feats = (ArrayNode) fc.get("features");
                    while (rs.next()) {
                        ObjectNode feat = om.createObjectNode();
                        feat.put("type", "Feature");

                        ObjectNode props = om.createObjectNode();
                        props.put("station_id", rs.getString("station_id"));
                        props.put("name", rs.getString("name"));
                        props.put("latest_date", String.valueOf(rs.getObject("latest_date")));
                        api.putNullable(props, "tmax_c", rs.getObject("tmax_c"));
                        api.putNullable(props, "tmin_c", rs.getObject("tmin_c"));
                        api.putNullable(props, "prcp_mm", rs.getObject("prcp_mm"));
                        if (rs.getObject("tmax_c") != null && rs.getObject("tmin_c") != null) {
                            double tmean = (rs.getDouble("tmax_c") + rs.getDouble("tmin_c")) / 2.0;
                            props.put("tmean_c", tmean);
                        }
                        feat.set("properties", props);

                        String geomJson = rs.getString("geom");
                        feat.set("geometry", geomJson == null ? om.nullNode() : om.readTree(geomJson));

                        feats.add(feat);
                    }
                }
            }

            try {
                api.cacheAndRespond(ctx, cacheKey, fc, 300, 600);
            } catch (Exception e) {
                ctx.json(fc);
            }
        });

        app.get("/api/gridpoints/list", ctx -> {
            String sql = """
                        SELECT grid_id, office, grid_x, grid_y,
                               ST_Y(geom) AS lat, ST_X(geom) AS lon,
                               forecast_hourly_url, last_refreshed_at
                        FROM geo_gridpoint
                        ORDER BY grid_id
                    """;

            ArrayNode arr = om.createArrayNode();
            try (Connection c = ds.getConnection();
                    PreparedStatement ps = c.prepareStatement(sql);
                    ResultSet rs = ps.executeQuery()) {

                while (rs.next()) {
                    ObjectNode row = om.createObjectNode();
                    row.put("grid_id", rs.getString("grid_id"));
                    row.put("office", rs.getString("office"));
                    row.put("grid_x", rs.getInt("grid_x"));
                    row.put("grid_y", rs.getInt("grid_y"));
                    row.put("lat", rs.getDouble("lat"));
                    row.put("lon", rs.getDouble("lon"));
                    row.put("forecast_hourly_url", rs.getString("forecast_hourly_url"));
                    row.put("last_refreshed_at", String.valueOf(rs.getObject("last_refreshed_at")));
                    arr.add(row);
                }
            }

            ctx.json(arr);
        });

        app.get("/api/alerts/active", ctx -> {
            String sql = """
                        SELECT alert_id, event, severity, certainty, urgency,
                               headline, effective, onset, expires, ends,
                               area_desc, ingested_at
                        FROM v_active_alerts
                        ORDER BY expires NULLS LAST, ingested_at DESC
                    """;

            ArrayNode arr = om.createArrayNode();
            try (Connection c = ds.getConnection();
                    PreparedStatement ps = c.prepareStatement(sql);
                    ResultSet rs = ps.executeQuery()) {

                while (rs.next()) {
                    ObjectNode row = om.createObjectNode();
                    row.put("alert_id", rs.getString("alert_id"));
                    row.put("event", rs.getString("event"));
                    row.put("severity", rs.getString("severity"));
                    row.put("certainty", rs.getString("certainty"));
                    row.put("urgency", rs.getString("urgency"));
                    row.put("headline", rs.getString("headline"));
                    row.put("effective", String.valueOf(rs.getObject("effective")));
                    row.put("onset", String.valueOf(rs.getObject("onset")));
                    row.put("expires", String.valueOf(rs.getObject("expires")));
                    row.put("ends", String.valueOf(rs.getObject("ends")));
                    row.put("area_desc", rs.getString("area_desc"));
                    row.put("ingested_at", String.valueOf(rs.getObject("ingested_at")));
                    arr.add(row);
                }
            }

            ctx.json(arr);
        });
    }
}
